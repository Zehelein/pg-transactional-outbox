import { ExtendedError, MessageError } from '../common/error';
import { ListenerConfig } from '../common/listener-config';
import { TransactionalLogger } from '../common/logger';
import { executeTransaction } from '../common/utils';
import { increaseMessageFinishedAttempts } from '../message/increase-message-finished-attempts';
import { markMessageAbandoned } from '../message/mark-message-abandoned';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { GeneralMessageHandler } from './general-message-handler';
import { HandlerStrategies } from './handler-strategies';
import { messageHandlerSelector } from './message-handler-selector';
import { TransactionalMessageHandler } from './transactional-message-handler';

/**
 * Create a function to execute the error handling orchestration logic for one
 * message. It executes the retry attempts logic and executes the message
 * handlers `handleError` function if it exists.
 * That executor function will not throw an error.
 */
export const createErrorHandler = (
  messageHandlers: TransactionalMessageHandler[] | GeneralMessageHandler,
  strategies: HandlerStrategies,
  config: ListenerConfig,
  logger: TransactionalLogger,
): ((
  message: StoredTransactionalMessage,
  error: ExtendedError,
) => Promise<boolean>) => {
  const handlerSelector = messageHandlerSelector(messageHandlers);
  /**
   * An error handler that will increase the "finished_attempts" counter.
   * The message handler `handleError` is called to allow your code to handle or
   * resolve the error.
   * @param message: the stored message that failed to be processed
   * @param error: the error that was thrown while processing the message
   * @returns A boolean value if the message should be retried again in the context of the replication listener
   */
  return async (
    message: StoredTransactionalMessage,
    error: ExtendedError,
  ): Promise<boolean> => {
    const handler = handlerSelector(message);
    const transactionLevel =
      strategies.messageProcessingTransactionLevelStrategy(message);
    let shouldRetry = true;
    try {
      return await executeTransaction(
        await strategies.messageProcessingDbClientStrategy.getClient(message),
        async (client) => {
          message.finishedAttempts++;
          shouldRetry = strategies.messageRetryStrategy(message, error);
          if (handler?.handleError) {
            await handler.handleError(error, message, client, shouldRetry);
          }
          if (shouldRetry) {
            await increaseMessageFinishedAttempts(message, client, config);
            logger.warn(
              {
                ...error,
                messageObject: message,
              },
              `An error ocurred while processing the ${config.outboxOrInbox} message with id ${message.id}.`,
            );
          } else {
            await markMessageAbandoned(message, client, config);
            logger.error(
              new MessageError(
                error.message,
                'GIVING_UP_MESSAGE_HANDLING',
                message,
                error,
              ),
              `Giving up processing the ${config.outboxOrInbox} message with id ${message.id}.`,
            );
          }
          return shouldRetry;
        },
        transactionLevel,
      );
    } catch (error) {
      const msg = handler
        ? `The error handling of the ${config.outboxOrInbox} message failed. Please make sure that your error handling code does not throw an error!`
        : `The error handling of the ${config.outboxOrInbox} message failed.`;
      logger.error(
        new MessageError(msg, 'MESSAGE_ERROR_HANDLING_FAILED', message, error),
        msg,
      );
      // In case the error handling logic failed do a best effort to increase the "finished_attempts" counter
      try {
        await executeTransaction(
          await strategies.messageProcessingDbClientStrategy.getClient(message),
          async (client) => {
            if (shouldRetry) {
              await increaseMessageFinishedAttempts(message, client, config);
            } else {
              await markMessageAbandoned(message, client, config);
            }
          },
          transactionLevel,
        );
        return shouldRetry;
      } catch (bestEffortError) {
        const e = new MessageError(
          `The 'best-effort' logic to increase the ${config.outboxOrInbox} message finished attempts failed as well.`,
          'MESSAGE_ERROR_HANDLING_FAILED',
          message,
          bestEffortError,
        );
        logger.warn(e, e.message);
        // If everything fails do not retry the message for logical replication to allow continuing with other messages
        // For polling it is unlikely that `markMessageAbandoned` would work now so just retry the message again is fine
        return false;
      }
    }
  };
};

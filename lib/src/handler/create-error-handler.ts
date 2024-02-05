import { ListenerConfig } from '../common/base-config';
import { ExtendedError, MessageError } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { executeTransaction } from '../common/utils';
import { increaseMessageFinishedAttempts } from '../message/increase-message-finished-attempts';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { GeneralMessageHandler } from './general-message-handler';
import { HandlerStrategies } from './handler-strategies';
import { messageHandlerSelector } from './message-handler-selector';
import { TransactionalMessageHandler } from './transactional-message-handler';

/**
 * Create the error handling orchestration logic to add retry attempts logic
 * and executes the message handler `handleError` function if it exists.
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
          shouldRetry = strategies.messageRetryStrategy(message);
          if (handler?.handleError) {
            await handler.handleError(error, message, client, shouldRetry);
          }
          if (shouldRetry) {
            logger.warn(
              {
                ...error,
                messageObject: message,
              },
              `An error ocurred while processing the ${config.outboxOrInbox} message with id ${message.id}.`,
            );
          } else {
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
          await increaseMessageFinishedAttempts(message, client, config);
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
            await increaseMessageFinishedAttempts(message, client, config);
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
        // If everything fails do not retry the message to allow continuing with other messages
        return false;
      }
    }
  };
};

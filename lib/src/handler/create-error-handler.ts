import { isPgSerializationError } from '../common/database';
import { ExtendedError, MessageError } from '../common/error';
import { ListenerConfig } from '../common/listener-config';
import { TransactionalLogger } from '../common/logger';
import { IsolationLevel, executeTransaction, sleep } from '../common/utils';
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
    try {
      return await executeTransaction(
        await strategies.messageProcessingDbClientStrategy.getClient(message),
        async (client) => {
          message.finishedAttempts++;
          const shouldRetry = strategies.messageRetryStrategy(
            message,
            error,
            'message-handler',
          );
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
              `An error ocurred while processing the ${config.outboxOrInbox} message with id ${message.id}. Retrying it again.`,
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
    } catch (err) {
      const msg = handler
        ? `The error handling of the ${config.outboxOrInbox} message failed. Please make sure that your error handling code does not throw an error!`
        : `The error handling of the ${config.outboxOrInbox} message failed.`;
      const error = new MessageError(
        msg,
        'MESSAGE_ERROR_HANDLING_FAILED',
        message,
        err,
      );
      const shouldRetry = strategies.messageRetryStrategy(
        message,
        error,
        'message-handler',
      );
      updateMessageText(error, shouldRetry);
      logger.error(error, error.message);
      // In case the error handling logic failed do a best effort to increase the "finished_attempts" counter or abandon the message
      try {
        await bestEffortMessageUpdate(
          strategies,
          message,
          shouldRetry,
          config,
          transactionLevel,
        );
        return strategies.messageRetryStrategy(message, error, 'error-handler');
      } catch (bestEffortError) {
        const e = new MessageError(
          `The 'best-effort' logic to increase the ${config.outboxOrInbox} message finished attempts failed as well.`,
          'MESSAGE_ERROR_HANDLING_FAILED',
          message,
          bestEffortError,
        );
        const shouldRetry = strategies.messageRetryStrategy(
          message,
          e,
          'error-handler-error',
        );
        updateMessageText(e, shouldRetry);
        logger.error(e, e.message);
        return shouldRetry;
      }
    }
  };
};

const updateMessageText = (e: Error, shouldRetry: boolean) =>
  (e.message = `${e.message} Attempting to ${
    shouldRetry ? 'retry' : 'abandon'
  } the message.`);

const bestEffortMessageUpdate = async (
  strategies: HandlerStrategies,
  message: StoredTransactionalMessage,
  shouldRetry: boolean,
  config: ListenerConfig,
  transactionLevel: IsolationLevel | undefined,
) => {
  let i = 0;
  do {
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
      return;
    } catch (error) {
      i++;
      // retry for serialization failure = 40001 and deadlock detected = 40P01 PG errors
      if (i < 3 && isPgSerializationError(error)) {
        await sleep(i * 100);
      } else {
        throw error;
      }
    }
  } while (i < 3);
};

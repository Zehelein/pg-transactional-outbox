import EventEmitter from 'events';
import { ListenerConfig } from '../common/base-config';
import { TransactionalOutboxInboxError } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { executeTransaction } from '../common/utils';
import { initiateMessageProcessing } from '../message/initiate-message-processing';
import { markMessageCompleted } from '../message/mark-message-completed';
import { startedAttemptsIncrement } from '../message/started-attempts-increment';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { GeneralMessageHandler } from './general-message-handler';
import { HandlerStrategies } from './handler-strategies';
import { messageHandlerSelector } from './message-handler-selector';
import { TransactionalMessageHandler } from './transactional-message-handler';

/**
 * Executes the message verification and poisonous message verification in one
 * transaction (if enabled) and the actual message handler and marking the
 * message as processed in another transaction.
 */
export const createMessageHandler = (
  messageHandlers: TransactionalMessageHandler[] | GeneralMessageHandler,
  strategies: HandlerStrategies,
  config: ListenerConfig,
  logger: TransactionalLogger,
): ((
  message: StoredTransactionalMessage,
  cancellation: EventEmitter,
) => Promise<void>) => {
  const handlerSelector = messageHandlerSelector(messageHandlers);
  return async (
    message: StoredTransactionalMessage,
    cancellation: EventEmitter,
  ): Promise<void> => {
    const handler = handlerSelector(message);
    if (!handler) {
      logger.debug(
        `No ${config.outboxOrInbox} message handler found for aggregate type "${message.aggregateType}" and message tye "${message.messageType}"`,
      );
    }
    const transactionLevel =
      strategies.messageProcessingTransactionLevelStrategy(message);

    if (handler && config.settings.enablePoisonousMessageProtection !== false) {
      const attempt = await executeTransaction(
        await strategies.messageProcessingDbClientStrategy.getClient(message),
        async (client) => {
          // Increment the started_attempts
          const result = await startedAttemptsIncrement(
            message,
            client,
            config,
          );
          if (result !== true) {
            logger.warn(
              message,
              `Could not increment the started attempts field of the received ${config.outboxOrInbox} message: ${result}`,
            );
            await client.query('ROLLBACK'); // don't increment the start attempts again on a processed message
            return false;
          }
          // The startedAttempts was incremented in `startedAttemptsIncrement` so the difference is always at least one
          const diff = message.startedAttempts - message.finishedAttempts;
          if (diff >= 2) {
            const retry = strategies.poisonousMessageRetryStrategy(message);
            if (!retry) {
              const msg = `Stopped processing the ${config.outboxOrInbox} message with ID ${message.id} as it is likely a poisonous message.`;
              logger.error(
                new TransactionalOutboxInboxError(msg, 'POISONOUS_MESSAGE'),
                msg,
              );
              return false;
            }
          }
          return true;
        },
        transactionLevel,
      );
      if (!attempt) {
        return;
      }
    }

    await executeTransaction(
      await strategies.messageProcessingDbClientStrategy.getClient(message),
      async (client) => {
        if (handler) {
          cancellation.on('timeout', async () => {
            await client.query('ROLLBACK');
            client.release();
          });

          // lock the message from further processing
          const result = await initiateMessageProcessing(
            message,
            client,
            config,
          );
          if (result !== true) {
            logger.warn(
              message,
              `The received ${config.outboxOrInbox} message cannot be processed: ${result}`,
            );
            return;
          }
          if (
            message.finishedAttempts > 0 &&
            !strategies.messageRetryStrategy(message)
          ) {
            logger.warn(
              message,
              `The received ${config.outboxOrInbox} message should not be retried`,
            );
            return;
          }
          await handler.handle(message, client);
        }
        await markMessageCompleted(message, client, config);
      },
      transactionLevel,
    );
  };
};

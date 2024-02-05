import EventEmitter from 'events';
import { TransactionalOutboxInboxError } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { executeTransaction } from '../common/utils';
import { initiateMessageProcessing } from '../message/initiate-message-processing';
import { markMessageCompleted } from '../message/mark-message-completed';
import { startedAttemptsIncrement } from '../message/started-attempts-increment';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { PollingConfig } from '../polling/config';
import {
  ReplicationConfig,
  ReplicationListenerConfig,
} from '../replication/config';
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
  config: PollingConfig | ReplicationConfig,
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

    if (handler && config.settings.enablePoisonousMessageProtection !== false) {
      if (isReplicationListener(config)) {
        const continueProcessing =
          await applyReplicationPoisonousMessageProtection(
            message,
            strategies,
            config,
            logger,
          );
        if (!continueProcessing) {
          return;
        }
      }
      // The startedAttempts was incremented in `startedAttemptsIncrement` or from the polling function
      // so the difference is always at least one
      const diff = message.startedAttempts - message.finishedAttempts;
      if (diff >= 2) {
        const retry = strategies.poisonousMessageRetryStrategy(message);
        if (!retry) {
          const msg = `Stopped processing the ${config.outboxOrInbox} message with ID ${message.id} as it is likely a poisonous message.`;
          logger.error(
            new TransactionalOutboxInboxError(msg, 'POISONOUS_MESSAGE'),
            msg,
          );
          return;
        }
      }
    }

    await startMessageProcessing(
      message,
      handler,
      strategies,
      cancellation,
      config,
      logger,
    );
  };
};

/**
 * When using the logical replication approach this function tries to increment
 * the started attempts of the message. This can then be compared to the
 * finished attempts to decide if the message should be retried.
 */
const applyReplicationPoisonousMessageProtection = async (
  message: StoredTransactionalMessage,
  strategies: HandlerStrategies,
  config: ReplicationConfig,
  logger: TransactionalLogger,
) => {
  const transactionLevel =
    strategies.messageProcessingTransactionLevelStrategy(message);
  return await executeTransaction(
    await strategies.messageProcessingDbClientStrategy.getClient(message),
    async (client) => {
      // Increment the started_attempts
      const result = await startedAttemptsIncrement(message, client, config);
      if (result !== true) {
        logger.warn(
          message,
          `Could not increment the started attempts field of the received ${config.outboxOrInbox} message: ${result}`,
        );
        await client.query('ROLLBACK'); // don't increment the start attempts again on a processed message
        return false;
      }
      return true;
    },
    transactionLevel,
  );
};

/** Lock the message and execute the handler (if there is any) and mark the message as completed */
const startMessageProcessing = async (
  message: StoredTransactionalMessage,
  handler: GeneralMessageHandler | undefined,
  strategies: HandlerStrategies,
  cancellation: EventEmitter,
  config: PollingConfig | ReplicationConfig,
  logger: TransactionalLogger,
) => {
  const transactionLevel =
    strategies.messageProcessingTransactionLevelStrategy(message);
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
          config.settings,
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

function isReplicationListener(
  config: PollingConfig | ReplicationConfig,
): config is ReplicationConfig {
  return !!(config.settings as ReplicationListenerConfig).postgresPub;
}

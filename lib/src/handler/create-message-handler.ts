import EventEmitter from 'events';
import { releaseIfPoolClient } from '../common/database';
import { TransactionalOutboxInboxError } from '../common/error';
import { ListenerConfig } from '../common/listener-config';
import { TransactionalLogger } from '../common/logger';
import { executeTransaction, justDoIt } from '../common/utils';
import { initiateMessageProcessing } from '../message/initiate-message-processing';
import { markMessageAbandoned } from '../message/mark-message-abandoned';
import { markMessageCompleted } from '../message/mark-message-completed';
import { startedAttemptsIncrement } from '../message/started-attempts-increment';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { PollingListenerConfig } from '../polling/config';
import { ReplicationListenerConfig } from '../replication/config';
import { GeneralMessageHandler } from './general-message-handler';
import { HandlerStrategies } from './handler-strategies';
import { messageHandlerSelector } from './message-handler-selector';
import { TransactionalMessageHandler } from './transactional-message-handler';

export type ListenerType = 'replication' | 'polling';

/**
 * Executes the message verification and poisonous message verification in one
 * transaction (if enabled) and the actual message handler and marking the
 * message as processed in another transaction.
 */
export const createMessageHandler = (
  messageHandlers: TransactionalMessageHandler[] | GeneralMessageHandler,
  strategies: HandlerStrategies,
  config: PollingListenerConfig | ReplicationListenerConfig,
  logger: TransactionalLogger,
  listenerType: ListenerType,
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
      if (listenerType === 'replication') {
        const continueProcessing =
          await applyReplicationPoisonousMessageProtection(
            message,
            strategies,
            config as ReplicationListenerConfig,
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
          await abandonPoisonousMessage(message, strategies, config);
          return;
        }
      }
    }

    await processMessage(
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
  config: ReplicationListenerConfig,
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

/**
 * Mark the message as abandoned if it is found to be (likely) a poisonous message
 */
const abandonPoisonousMessage = async (
  message: StoredTransactionalMessage,
  strategies: HandlerStrategies,
  config: ListenerConfig,
) => {
  const transactionLevel =
    strategies.messageProcessingTransactionLevelStrategy(message);
  return await executeTransaction(
    await strategies.messageProcessingDbClientStrategy.getClient(message),
    async (client) => {
      await markMessageAbandoned(message, client, config);
    },
    transactionLevel,
  );
};

/** Lock the message and execute the handler (if there is any) and mark the message as completed */
const processMessage = async (
  message: StoredTransactionalMessage,
  handler: GeneralMessageHandler | undefined,
  strategies: HandlerStrategies,
  cancellation: EventEmitter,
  config: PollingListenerConfig | ReplicationListenerConfig,
  logger: TransactionalLogger,
) => {
  const transactionLevel =
    strategies.messageProcessingTransactionLevelStrategy(message);
  await executeTransaction(
    await strategies.messageProcessingDbClientStrategy.getClient(message),
    async (client) => {
      let timedOut = false;
      if (handler) {
        cancellation.on('timeout', async () => {
          timedOut = true;
          // roll back the current changes and release/end the client to disable further changes
          await client.query('ROLLBACK');
          if ('release' in client) {
            client.release(
              new TransactionalOutboxInboxError(
                'Message processing timeout',
                'TIMEOUT',
              ),
            );
          } else if ('end' in client) {
            await client.end();
          }
          await justDoIt(() => {
            releaseIfPoolClient(client);
          });
        });

        // lock the message from further processing
        const result = await initiateMessageProcessing(
          message,
          client,
          config.settings,
          strategies.messageNotFoundRetryStrategy,
        );
        if (result !== true) {
          logger.warn(
            message,
            `The received ${config.outboxOrInbox} message cannot be processed: ${result}`,
          );
          return;
        }
        await handler.handle(message, client);
      }
      if (!timedOut) {
        await markMessageCompleted(message, client, config);
      }
    },
    transactionLevel,
  );
};

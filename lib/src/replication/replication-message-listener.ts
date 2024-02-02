import EventEmitter from 'events';
import { PoolClient } from 'pg';
import {
  ExtendedError,
  MessageError,
  TransactionalOutboxInboxError,
} from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { executeTransaction } from '../common/utils';
import { StoredTransactionalMessage } from '../message/message';
import {
  increaseMessageFinishedAttempts,
  initiateMessageProcessing,
  markMessageCompleted,
  startedAttemptsIncrement,
} from '../message/message-storage';
import { defaultConcurrencyStrategy } from '../strategies/concurrency-strategy';
import { defaultListenerRestartStrategy } from '../strategies/listener-restart-strategy';
import { defaultMessageProcessingDbClientStrategy } from '../strategies/message-processing-db-client-strategy';
import { defaultMessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';
import { defaultMessageProcessingTransactionLevelStrategy } from '../strategies/message-processing-transaction-level-strategy';
import { defaultMessageRetryStrategy } from '../strategies/message-retry-strategy';
import { defaultPoisonousMessageRetryStrategy } from '../strategies/poisonous-message-retry-strategy';
import { ReplicationConfig } from './config';
import { createLogicalReplicationListener } from './logical-replication-listener';
import { ReplicationMessageStrategies } from './replication-strategies';

/**
 * Message handler for a specific aggregate type and message type.
 */
export interface TransactionalMessageHandler {
  /** The aggregate root type */
  aggregateType: string;

  /** The name of the message created for the aggregate type. */
  messageType: string;

  /**
   * Custom business logic to handle a message that was stored in the
   * transactional table. It is fine to throw an error if the message cannot be
   * processed.
   * @param message The message with the payload to handle.
   * @param client The database client that is part of a transaction to safely handle the message.
   * @throws If something failed and the message should NOT be acknowledged - throw an error.
   */
  handle: (
    message: StoredTransactionalMessage,
    client: PoolClient,
  ) => Promise<void>;

  /**
   * Custom (optional) business logic to handle an error that was caused by the
   * "handle" method. Please ensure that this function does not throw an error
   * as the finished_attempts counter is increased in the same transaction.
   * @param error The error that was thrown in the handle method.
   * @param message The message with the payload that was attempted to be handled. The finishedAttempts property includes the number of processing attempts - including the current one if DB retries management is activated.
   * @param client The database client that is part of a (new) transaction to safely handle the error.
   * @param retry True if the message should be retried again.
   */
  handleError?: (
    error: ExtendedError,
    message: StoredTransactionalMessage,
    client: PoolClient,
    retry: boolean,
  ) => Promise<void>;
}

/**
 * Message handler for handling all aggregate types and message types.
 */
export interface GeneralMessageHandler {
  /**
   * Custom business logic to handle a message that was stored in the
   * transactional table. It is fine to throw an error if the message cannot be
   * processed.
   * @param message The message with the payload to handle.
   * @param client The database client that is part of a transaction to safely handle the message.
   * @throws If something failed and the message should NOT be acknowledged - throw an error.
   */
  handle: (
    message: StoredTransactionalMessage,
    client: PoolClient,
  ) => Promise<void>;

  /**
   * Custom (optional) business logic to handle an error that was caused by the
   * "handle" method. Please ensure that this function does not throw an error
   * as the finished_attempts counter is increased in the same transaction.
   * @param error The error that was thrown in the handle method.
   * @param message The message with the payload that was attempted to be handled. The finishedAttempts property includes the number of processing attempts - including the current one if DB retries management is activated.
   * @param client The database client that is part of a (new) transaction to safely handle the error.
   * @param retry True if the message should be retried again.
   */
  handleError?: (
    error: ExtendedError,
    message: StoredTransactionalMessage,
    client: PoolClient,
    retry: boolean,
  ) => Promise<void>;
}

/**
 * Initialize the listener to watch for outbox or inbox table inserts via
 * PostgreSQL logical replication.
 * @param config The configuration object with required values to connect to the WAL.
 * @param messageHandlers A list of message handlers to handle specific messages or a single general message handler that handles all messages.
 * @param logger A logger instance for logging trace up to error logs
 * @param strategies Strategies to provide custom logic for handling specific scenarios
 * @returns Functions for a clean shutdown.
 */
export const initializeReplicationMessageListener = (
  config: ReplicationConfig,
  messageHandlers: TransactionalMessageHandler[] | GeneralMessageHandler,
  logger: TransactionalLogger,
  strategies?: Partial<ReplicationMessageStrategies>,
): [shutdown: { (): Promise<void> }] => {
  const allStrategies = applyDefaultStrategies(strategies, config, logger);
  const handlerSelector = Array.isArray(messageHandlers)
    ? createMessageHandlerDict(messageHandlers)
    : messageHandlers;
  const messageHandler = createMessageHandler(
    handlerSelector,
    allStrategies,
    config,
    logger,
  );
  const errorHandler = createErrorHandler(
    handlerSelector,
    allStrategies,
    config,
    logger,
  );
  const [shutdown] = createLogicalReplicationListener(
    config,
    messageHandler,
    errorHandler,
    logger,
    allStrategies,
  );
  return [
    async () => {
      await Promise.all([
        allStrategies.messageProcessingDbClientStrategy?.shutdown(),
        shutdown(),
      ]);
    },
  ];
};

/**
 * Executes the message verification, the actual message handler, and marks the
 * message as processed in one transaction.
 */
const createMessageHandler = (
  handlerSelector:
    | Record<string, TransactionalMessageHandler>
    | GeneralMessageHandler,
  strategies: ReplicationMessageStrategies,
  config: ReplicationConfig,
  logger: TransactionalLogger,
) => {
  return async (
    message: StoredTransactionalMessage,
    cancellation: EventEmitter,
  ) => {
    const transactionLevel =
      strategies.messageProcessingTransactionLevelStrategy(message);

    if (config.settings.enablePoisonousMessageProtection !== false) {
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
        cancellation.on('timeout', async () => {
          await client.query('ROLLBACK');
          client.release();
        });

        // lock the message from further processing
        const result = await initiateMessageProcessing(message, client, config);
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

        // Execute the message handler
        const handler = selectMessageHandler(message, handlerSelector);
        if (handler) {
          await handler.handle(message, client);
        } else {
          logger.debug(
            `No ${config.outboxOrInbox} message handler found for aggregate type "${message.aggregateType}" and message tye "${message.messageType}"`,
          );
        }
        await markMessageCompleted(message, client, config);
      },
      transactionLevel,
    );
  };
};

const getMessageHandlerKey = (
  h: Pick<StoredTransactionalMessage, 'aggregateType' | 'messageType'>,
) => `${h.aggregateType}@${h.messageType}`;

const createMessageHandlerDict = (
  messageHandlers: TransactionalMessageHandler[],
): Record<string, TransactionalMessageHandler> => {
  const handlers: Record<string, TransactionalMessageHandler> = {};
  for (const handler of messageHandlers) {
    const key = getMessageHandlerKey(handler);
    if (key in handlers) {
      throw new TransactionalOutboxInboxError(
        `Only one message handler can handle one aggregate and message type. Multiple message handlers try to handle the aggregate type "${handler.aggregateType}" with the message type "${handler.messageType}".`,
        'CONFLICTING_MESSAGE_HANDLERS',
      );
    }
    handlers[key] = handler;
  }

  if (Object.keys(handlers).length === 0) {
    throw new TransactionalOutboxInboxError(
      'At least one message handler must be provided.',
      'NO_MESSAGE_HANDLER_REGISTERED',
    );
  }

  return handlers;
};

function isGeneralMessageHandler(
  handlerSelector:
    | Record<string, TransactionalMessageHandler>
    | GeneralMessageHandler,
): handlerSelector is GeneralMessageHandler {
  return !!(handlerSelector as GeneralMessageHandler).handle;
}

const selectMessageHandler = (
  message: StoredTransactionalMessage,
  handlerSelector:
    | Record<string, TransactionalMessageHandler>
    | GeneralMessageHandler,
): GeneralMessageHandler => {
  if (isGeneralMessageHandler(handlerSelector)) {
    return handlerSelector;
  } else {
    return handlerSelector[getMessageHandlerKey(message)];
  }
};

/**
 * Increase the "finished_attempts" counter of the message and (potentially) retry it later.
 */
const createErrorHandler = (
  handlerSelector:
    | Record<string, TransactionalMessageHandler>
    | GeneralMessageHandler,
  strategies: ReplicationMessageStrategies,
  config: ReplicationConfig,
  logger: TransactionalLogger,
) => {
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
    const handler = selectMessageHandler(message, handlerSelector);
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
              `An error ocurred while processing the message with id ${message.id}.`,
            );
          } else {
            logger.error(
              new MessageError(
                error.message,
                'GIVING_UP_MESSAGE_HANDLING',
                message,
                error,
              ),
              `Giving up processing the message with id ${message.id}.`,
            );
          }
          await increaseMessageFinishedAttempts(message, client, config);
          return shouldRetry;
        },
        transactionLevel,
      );
    } catch (error) {
      const msg = handler
        ? 'The error handling of the message failed. Please make sure that your error handling code does not throw an error!'
        : 'The error handling of the message failed.';
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
          "The 'best-effort' logic to increase the finished attempts failed as well.",
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

const applyDefaultStrategies = (
  strategies: Partial<ReplicationMessageStrategies> | undefined,
  config: ReplicationConfig,
  logger: TransactionalLogger,
): ReplicationMessageStrategies => ({
  concurrencyStrategy:
    strategies?.concurrencyStrategy ?? defaultConcurrencyStrategy(),
  messageProcessingDbClientStrategy:
    strategies?.messageProcessingDbClientStrategy ??
    defaultMessageProcessingDbClientStrategy(config, logger),
  messageProcessingTimeoutStrategy:
    strategies?.messageProcessingTimeoutStrategy ??
    defaultMessageProcessingTimeoutStrategy(config),
  messageProcessingTransactionLevelStrategy:
    strategies?.messageProcessingTransactionLevelStrategy ??
    defaultMessageProcessingTransactionLevelStrategy(),
  messageRetryStrategy:
    strategies?.messageRetryStrategy ?? defaultMessageRetryStrategy(config),
  poisonousMessageRetryStrategy:
    strategies?.poisonousMessageRetryStrategy ??
    defaultPoisonousMessageRetryStrategy(config),
  listenerRestartStrategy:
    strategies?.listenerRestartStrategy ??
    defaultListenerRestartStrategy(config),
});

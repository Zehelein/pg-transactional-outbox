import { ClientBase, ClientConfig } from 'pg';
import {
  ExtendedError,
  MessageError,
  TransactionalOutboxInboxError,
  ensureExtendedError,
} from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { InboxMessage } from '../common/message';
import { executeTransaction } from '../common/utils';
import { TransactionalOutboxInboxConfig } from '../replication/config';
import {
  TransactionalStrategies,
  createLogicalReplicationListener,
} from '../replication/logical-replication-listener';
import { defaultConcurrencyStrategy } from '../strategies/concurrency-strategy';
import { defaultListenerRestartStrategy } from '../strategies/listener-restart-strategy';
import {
  MessageProcessingDbClientStrategy,
  defaultMessageProcessingDbClientStrategy,
} from '../strategies/message-processing-db-client-strategy';
import { defaultMessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';
import {
  MessageProcessingTransactionLevelStrategy,
  defaultMessageProcessingTransactionLevelStrategy,
} from '../strategies/message-processing-transaction-level-strategy';
import {
  MessageRetryStrategy,
  defaultMessageRetryStrategy,
} from '../strategies/message-retry-strategy';
import {
  PoisonousMessageRetryStrategy,
  defaultPoisonousMessageRetryStrategy,
} from '../strategies/poisonous-message-retry-strategy';
import {
  increaseInboxMessageFinishedAttempts,
  initiateInboxMessageProcessing,
  markInboxMessageCompleted,
  startedAttemptsIncrement,
} from './inbox-message-storage';

/** The inbox listener configuration */
export type InboxConfig = TransactionalOutboxInboxConfig & {
  /**
   * Database connection details. The user needs update permission to the inbox.
   */
  pgConfig: ClientConfig;

  settings: TransactionalOutboxInboxConfig['settings'] & {
    /**
     * The maximum number of attempts to handle an incoming inbox message.
     * Defaults to 5 which means a message is handled once initially and up to
     * four more times for retries.
     */
    maxAttempts?: number;
    /**
     * Defines the maximum number of times a message is going to be processed
     * when it is (likely) a poisonous message which is causing a server crash.
     * This is used in the default poisonous message retry strategy.
     * It defaults to three.
     */
    maxPoisonousAttempts?: number;
    /**
     * Poisonous message protection is enabled by default or if you set this to
     * true. Enabling it will take a little bit more time but will prevent an
     * infinite service crash loop if there is a poisonous message.
     */
    enablePoisonousMessageProtection?: boolean;
  };
};

/**
 * Message handler for a specific aggregate type and message type.
 */
export interface InboxMessageHandler {
  /** The aggregate root type */
  aggregateType: string;

  /** The name of the message created for the aggregate type. */
  messageType: string;

  /**
   * Custom business logic to handle a message that was stored in the inbox. It
   * is fine to throw an error if the message cannot be processed.
   * @param message The inbox message with the payload to handle.
   * @param client The database client that is part of a transaction to safely handle the inbox message.
   * @throws If something failed and the inbox message should NOT be acknowledged - throw an error.
   */
  handle: (message: InboxMessage, client: ClientBase) => Promise<void>;

  /**
   * Custom (optional) business logic to handle an error that was caused by the
   * "handle" method. Please ensure that this function does not throw an error
   * as the finished_attempts counter is increased in the same transaction.
   * @param error The error that was thrown in the handle method.
   * @param message The inbox message with the payload that was attempted to be handled. The finishedAttempts property includes the number of processing attempts - including the current one.
   * @param client The database client that is part of a (new) transaction to safely handle the error.
   * @param retry True if the message should be retried again.
   * @returns A flag that defines if the message should be retried ('transient_error') or not ('permanent_error')
   */
  handleError?: (
    error: ExtendedError,
    message: InboxMessage,
    client: ClientBase,
    retry: boolean,
  ) => Promise<void>;
}

/** Strategies to provide the desired logic for handling specific scenarios */
export interface InboxStrategies extends TransactionalStrategies {
  /**
   * Defines the PostgreSQL transaction level to use for handling an incoming
   * inbox message.
   */
  messageProcessingTransactionLevelStrategy: MessageProcessingTransactionLevelStrategy;

  /**
   * Decides from what pool to take a database client. This can be helpful if some
   * message handlers have to run in higher trust roles than others.
   */
  messageProcessingDbClientStrategy: MessageProcessingDbClientStrategy;

  /**
   * Decides if a message should be retried or not based on the current amount
   * of attempts.
   */
  messageRetryStrategy: MessageRetryStrategy;

  /**
   * Decide based on the message, the poisonous attempts counter (which is
   * already increased by one), and the processing attempts if the message
   * should be retried again or not.
   */
  poisonousMessageRetryStrategy: PoisonousMessageRetryStrategy;
}

/**
 * Initialize the listener to watch for inbox table inserts.
 * @param config The configuration object with required values to connect to the WAL.
 * @param messageHandlers A list of message handlers to handle the inbox messages.
 * @param logger A logger instance for logging trace up to error logs
 * @param strategies Strategies to provide custom logic for handling specific scenarios
 * @returns Functions for a clean shutdown.
 */
export const initializeInboxListener = (
  config: InboxConfig,
  messageHandlers: InboxMessageHandler[],
  logger: TransactionalLogger,
  strategies?: Partial<InboxStrategies>,
): [shutdown: { (): Promise<void> }] => {
  const allStrategies = applyDefaultStrategies(strategies, config, logger);
  const messageHandlerDict = getMessageHandlerDict(messageHandlers);
  const messageHandler = createMessageHandler(
    messageHandlerDict,
    allStrategies,
    config,
    logger,
  );
  const errorResolver = createErrorResolver(
    messageHandlerDict,
    allStrategies,
    config,
    logger,
  );
  const [shutdown] = createLogicalReplicationListener(
    config,
    messageHandler,
    errorResolver,
    logger,
    allStrategies,
    'inbox',
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
 * Executes the inbox verification, the actual message handler, and marks the
 * inbox message as processed in one transaction.
 */
const createMessageHandler = (
  messageHandlers: Record<string, InboxMessageHandler>,
  strategies: InboxStrategies,
  config: InboxConfig,
  logger: TransactionalLogger,
) => {
  return async (message: InboxMessage) => {
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
              `Could not increment the started attempts field of the received inbox message: ${result}`,
            );
            await client.query('ROLLBACK'); // don't increment the start attempts again on a processed message
            return false;
          }
          // The startedAttempts was incremented in `startedAttemptsIncrement` so the difference is always at least one
          const diff = message.startedAttempts - message.finishedAttempts;
          if (diff >= 2) {
            const retry = strategies.poisonousMessageRetryStrategy(message);
            if (!retry) {
              const msg = `Stopped processing the message with ID ${message.id} as it is likely a poisonous message.`;
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
        // lock the inbox message from further processing
        const result = await initiateInboxMessageProcessing(
          message,
          client,
          config,
        );
        if (result !== true) {
          logger.warn(
            message,
            `Received inbox message cannot be processed: ${result}`,
          );
          return;
        }
        if (
          message.finishedAttempts > 0 &&
          !strategies.messageRetryStrategy(message)
        ) {
          logger.warn(message, `Received inbox message should not be retried`);
          return;
        }

        // Execute the message handler
        const handler = messageHandlers[getMessageHandlerKey(message)];
        if (handler) {
          await handler.handle(message, client);
        } else {
          logger.debug(
            `No message handler found for aggregate type "${message.aggregateType}" and message tye "${message.messageType}"`,
          );
        }
        await markInboxMessageCompleted(message, client, config);
      },
      transactionLevel,
    );
  };
};

const getMessageHandlerKey = (
  h: Pick<InboxMessage, 'aggregateType' | 'messageType'>,
) => `${h.aggregateType}@${h.messageType}`;

const getMessageHandlerDict = (
  messageHandlers: InboxMessageHandler[],
): Record<string, InboxMessageHandler> => {
  const handlers: Record<string, InboxMessageHandler> = {};
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

/**
 * Increase the "finished_attempts" counter of the message and (potentially) retry it later.
 */
const createErrorResolver = (
  messageHandlers: Record<string, InboxMessageHandler>,
  strategies: InboxStrategies,
  config: InboxConfig,
  logger: TransactionalLogger,
) => {
  /**
   * An error handler that will increase the "finished_attempts" counter on
   * transient errors or sets the "finished_attempts" counter to max attempts
   * for permanent errors. The message handler `handleError` is called to allow
   * your code to decide if the error is a transient or a permanent one.
   * @param message: the InboxMessage that failed to be processed
   * @param error: the error that was thrown while processing the message
   */
  return async (
    message: InboxMessage,
    error: ExtendedError,
  ): Promise<boolean> => {
    const handler = messageHandlers[getMessageHandlerKey(message)];
    const transactionLevel =
      strategies.messageProcessingTransactionLevelStrategy(message);
    try {
      return await executeTransaction(
        await strategies.messageProcessingDbClientStrategy.getClient(message),
        async (client) => {
          message.finishedAttempts++;
          const shouldRetry = strategies.messageRetryStrategy(message);
          if (handler?.handleError) {
            await handler.handleError(error, message, client, shouldRetry);
          }
          if (shouldRetry) {
            logger.warn(
              error,
              `An error ocurred while processing the message with id ${message.id}.`,
            );
          } else {
            logger.error(
              ensureExtendedError(error, 'GIVING_UP_MESSAGE_HANDLING'),
              `Giving up processing the message with id ${message.id}.`,
            );
          }
          await increaseInboxMessageFinishedAttempts(message, client, config);
          return shouldRetry;
        },
        transactionLevel,
      );
    } catch (error) {
      const msg = handler
        ? 'The error handling of the message failed. Please make sure that your error handling code does not throw an error!'
        : 'The error handling of the message failed.';
      logger.error(
        new MessageError(msg, 'MESSAGE_HANDLING_FAILED', message, error),
        msg,
      );
      // In case the error handling logic failed do a best effort to increase the "finished_attempts" counter
      try {
        await executeTransaction(
          await strategies.messageProcessingDbClientStrategy.getClient(message),
          async (client) => {
            await increaseInboxMessageFinishedAttempts(message, client, config);
          },
          transactionLevel,
        );
        return true;
      } catch {
        return false;
      }
    }
  };
};

const applyDefaultStrategies = (
  strategies: Partial<InboxStrategies> | undefined,
  config: InboxConfig,
  logger: TransactionalLogger,
): InboxStrategies => ({
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

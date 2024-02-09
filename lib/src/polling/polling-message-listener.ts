import EventEmitter from 'events';
import { Pool } from 'pg';
import { ExtendedError, ensureExtendedError } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import {
  awaitWithTimeout,
  justDoIt,
  processPool,
  sleep,
} from '../common/utils';
import { createErrorHandler } from '../handler/create-error-handler';
import { createMessageHandler } from '../handler/create-message-handler';
import { GeneralMessageHandler } from '../handler/general-message-handler';
import { TransactionalMessageHandler } from '../handler/transactional-message-handler';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { defaultMessageProcessingDbClientStrategy } from '../strategies/message-processing-db-client-strategy';
import { defaultMessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';
import { defaultMessageProcessingTransactionLevelStrategy } from '../strategies/message-processing-transaction-level-strategy';
import { defaultMessageRetryStrategy } from '../strategies/message-retry-strategy';
import { defaultPoisonousMessageRetryStrategy } from '../strategies/poisonous-message-retry-strategy';
import { PollingConfig } from './config';
import { getNextInboxMessages } from './next-messages';
import { PollingMessageStrategies } from './polling-strategies';
import { defaultPollingListenerBatchSizeStrategy } from './strategies/batch-size-strategy';

/**
 * Initialize the listener to watch for outbox or inbox table inserts via
 * polling the corresponding PostgreSQL database tables.
 * @param config The configuration object with required values to connect to the database for polling.
 * @param messageHandlers A list of message handlers to handle specific messages or a single general message handler that handles all messages.
 * @param logger A logger instance for logging trace up to error logs
 * @param strategies Strategies to provide custom logic for handling specific scenarios
 * @returns Functions for a clean shutdown.
 */
export const initializePollingMessageListener = (
  config: PollingConfig,
  messageHandlers: TransactionalMessageHandler[] | GeneralMessageHandler,
  logger: TransactionalLogger,
  strategies?: Partial<PollingMessageStrategies>,
): [shutdown: { (): Promise<void> }] => {
  const allStrategies = applyDefaultStrategies(strategies, config, logger);
  const messageHandler = createMessageHandler(
    messageHandlers,
    allStrategies,
    config,
    logger,
    'polling',
  );
  const errorHandler = createErrorHandler(
    messageHandlers,
    allStrategies,
    config,
    logger,
  );
  let pool: Pool = undefined as unknown as Pool;

  // Start the asynchronous background polling loop
  // TODO: add an event emitter for a global stop on shutdown
  // TODO: implement LISTEN/NOTIFY to immediately run the next interval (when workers are free)
  const signal = { stopped: false };
  (function start() {
    logger.debug(`Start polling for ${config.outboxOrInbox} messages.`);
    if (pool) {
      justDoIt(pool.end);
    }
    pool = new Pool(config.dbListenerConfig);
    pool.on('error', (error) => {
      logger.error(
        ensureExtendedError(error, 'DB_ERROR'),
        'PostgreSQL pool error',
      );
    });
    pool.on('connect', (client) => {
      client.removeAllListeners('notice');
      client.on('notice', (msg) => {
        logger.trace('raised notice', msg.message);
      });
    });
    const applyRestart = (promise: Promise<unknown>) => {
      void promise.catch(async (e) => {
        const err = ensureExtendedError(e, 'LISTENER_STOPPED');
        logger.error(
          err,
          `Error polling for ${config.outboxOrInbox} messages.`,
        );
        if (!signal.stopped) {
          await sleep(1000);
          setImmediate(start); // restart
        }
      });
    };

    const getNextBatch = async (batchSize: number) =>
      processBatch(
        batchSize,
        config,
        pool,
        messageHandler,
        errorHandler,
        allStrategies,
        logger,
      );

    const processingPool: Set<Promise<void>> = new Set();
    applyRestart(
      processPool(
        processingPool,
        getNextBatch,
        allStrategies.batchSizeStrategy,
        signal,
        config.settings.nextMessagesPollingInterval ?? 500,
      ),
    );
  })();

  return [
    async () => {
      signal.stopped = true;
      await Promise.all([
        allStrategies.messageProcessingDbClientStrategy?.shutdown(),
        pool?.end(),
      ]);
    },
  ];
};

const applyDefaultStrategies = (
  strategies: Partial<PollingMessageStrategies> | undefined,
  config: PollingConfig,
  logger: TransactionalLogger,
): PollingMessageStrategies => ({
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
  batchSizeStrategy:
    strategies?.batchSizeStrategy ??
    defaultPollingListenerBatchSizeStrategy(config),
});

const processBatch = async (
  batchSize: number,
  config: PollingConfig,
  pool: Pool,
  messageHandler: (
    message: StoredTransactionalMessage,
    cancellation: EventEmitter,
  ) => Promise<void>,
  errorHandler: (
    message: StoredTransactionalMessage,
    error: ExtendedError,
  ) => Promise<boolean>,
  allStrategies: PollingMessageStrategies,
  logger: TransactionalLogger,
): Promise<Promise<void>[]> => {
  let messages: StoredTransactionalMessage[] = [];
  try {
    messages = await getNextInboxMessages(
      batchSize,
      pool,
      config.settings,
      logger,
    );
    // Messages can be processed in parallel
    const batchPromises = messages.map(async (message) => {
      const cancellation = new EventEmitter();
      try {
        const messageProcessingTimeout =
          allStrategies.messageProcessingTimeoutStrategy(message);
        await awaitWithTimeout(
          async () => {
            logger.debug(
              message,
              `Executing the message handler for message with ID ${message.id}.`,
            );
            await messageHandler(message, cancellation);
          },
          messageProcessingTimeout,
          `Could not process the message with id ${message.id} within the timeout of ${messageProcessingTimeout} milliseconds. Please consider to use a background worker for long running tasks to not block the message processing.`,
        );
        logger.trace(
          message,
          `Finished processing the message with id ${message.id} and type ${message.messageType}.`,
        );
      } catch (e) {
        const err = ensureExtendedError(e, 'MESSAGE_HANDLING_FAILED');
        if (err.errorCode === 'TIMEOUT') {
          cancellation.emit('timeout', err);
        }
        logger.warn(
          message,
          `Message processing error for the message with id ${message.id} and type ${message.messageType}.`,
        );
        await errorHandler(message, err);
      }
    });
    return batchPromises;
  } catch (batchError) {
    const error = ensureExtendedError(batchError, 'BATCH_PROCESSING_ERROR');
    logger.trace(
      { ...error, messages },
      `Error when working on a batch of ${config.outboxOrInbox} messages.`,
    );
    return [];
  }
};

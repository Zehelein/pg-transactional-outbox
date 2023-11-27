import * as dotenv from 'dotenv';
import path from 'path';
dotenv.config({ path: path.join(__dirname, '../.env') });
// eslint-disable-next-line prettier/prettier
import {
  IsolationLevel,
  createMutexConcurrencyController,
  initializeInboxListener,
  initializeInboxMessageStorage,
} from 'pg-transactional-outbox';
import { getConfig, getInboxConfig } from './config';
import { getLogger } from './logger';
import { initializeRabbitMqHandler } from './rabbitmq-handler';
import {
  MovieAggregateType,
  MovieCreatedMessageType,
  getStorePublishedMovie,
} from './receive-movie';

const logger = getLogger();

// Exit the process if there is an unhandled promise error
process.on('unhandledRejection', (err, promise) => {
  logger.error({ err, promise }, 'Unhandled promise rejection');
  process.exit(1);
});

/** The main entry point of the message producer. */
(async () => {
  const config = getConfig();
  const inboxConfig = getInboxConfig(config);

  // Initialize the inbox message storage to store incoming messages in the inbox
  const [storeInboxMessage, shutdownInStore] = initializeInboxMessageStorage(
    inboxConfig,
    logger,
  );

  // Initialize the RabbitMQ message handler to receive messages and store them in the inbox
  const [shutdownRmq] = await initializeRabbitMqHandler(
    config,
    storeInboxMessage,
    [MovieCreatedMessageType],
    logger,
  );

  // Initialize and start the inbox subscription
  const storePublishedMovie = getStorePublishedMovie(logger);
  const [shutdownInSrv] = initializeInboxListener(
    inboxConfig,
    [
      {
        aggregateType: MovieAggregateType,
        messageType: MovieCreatedMessageType,
        handle: storePublishedMovie,
      },
    ],
    logger,
    {
      concurrencyStrategy: createMutexConcurrencyController(),
      messageProcessingTimeoutStrategy: () => 2000,
      messageProcessingTransactionLevel: () => IsolationLevel.Serializable,
    },
  );

  // Close all connections
  const cleanup = async () => {
    await Promise.allSettled([
      shutdownInStore(),
      shutdownRmq(),
      shutdownInSrv(),
    ]);
    process.exit(0);
  };
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);
})();

import * as dotenv from 'dotenv';
import path from 'path';
dotenv.config({ path: path.join(__dirname, '../.env') });
// eslint-disable-next-line prettier/prettier
import {
  createMutexConcurrencyController,
  initializeInboxMessageStorage,
  initializeInboxService,
} from 'pg-transactional-outbox';
import { getConfig, getInboxServiceConfig } from './config';
import { getLogger } from './logger';
import { initializeRabbitMqHandler } from './rabbitmq-handler';
import {
  getStorePublishedMovie,
  MovieAggregateType,
  MovieCreatedMessageType,
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
  const inboxConfig = getInboxServiceConfig(config);

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
  const [shutdownInSrv] = initializeInboxService(
    inboxConfig,
    [
      {
        aggregateType: MovieAggregateType,
        messageType: MovieCreatedMessageType,
        handle: storePublishedMovie,
      },
    ],
    logger,
    createMutexConcurrencyController(),
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

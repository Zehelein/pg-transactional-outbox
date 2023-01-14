import path from 'path';
import * as dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '../.env') });
import {
  initializeInboxMessageStorage,
  initializeInboxService,
  setLogger,
} from 'pg-transactional-outbox';
import { getConfig, getInboxServiceConfig } from './config';
import { logger } from './logger';
import { initializeRabbitMqHandler } from './rabbitmq-handler';
import {
  MovieAggregateType,
  MovieCreatedEventType,
  storePublishedMovie,
} from './receive-movie';

// Exit the process if there is an unhandled promise error
process.on('unhandledRejection', (err, promise) => {
  logger.error({ err, promise }, 'Unhandled promise rejection');
  process.exit(1);
});

/** The main entry point of the message producer. */
(async () => {
  // Set the pino logger also for the library logging
  setLogger(logger);
  const config = getConfig();
  const inboxConfig = getInboxServiceConfig(config);

  // Initialize the inbox message storage to store incoming messages in the inbox
  const [storeInboxMessage, shutdownInStore] =
    await initializeInboxMessageStorage(inboxConfig);

  // Initialize the RabbitMQ message handler to receive messages and store them in the inbox
  const [shutdownRmq] = await initializeRabbitMqHandler(
    config,
    storeInboxMessage,
    [MovieCreatedEventType],
  );

  // Initialize and start the inbox subscription
  const [shutdownInSrv] = await initializeInboxService(inboxConfig, [
    {
      aggregateType: MovieAggregateType,
      eventType: MovieCreatedEventType,
      handle: storePublishedMovie,
    },
  ]);

  // Close all connections
  const cleanup = async () =>
    Promise.allSettled([shutdownInStore, shutdownRmq, shutdownInSrv]);
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);
})();

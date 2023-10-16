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
  MovieCreatedMessageType,
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
    initializeInboxMessageStorage(inboxConfig);

  // Initialize the RabbitMQ message handler to receive messages and store them in the inbox
  const [shutdownRmq] = await initializeRabbitMqHandler(
    config,
    storeInboxMessage,
    [MovieCreatedMessageType],
  );

  // Initialize and start the inbox subscription
  const [shutdownInSrv] = initializeInboxService(inboxConfig, [
    {
      aggregateType: MovieAggregateType,
      messageType: MovieCreatedMessageType,
      handle: storePublishedMovie,
    },
  ]);

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

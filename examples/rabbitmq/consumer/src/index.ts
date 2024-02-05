import * as dotenv from 'dotenv';
import path from 'path';
dotenv.config({ path: path.join(__dirname, '../.env') });
// eslint-disable-next-line prettier/prettier
import {
  IsolationLevel,
  createReplicationMutexConcurrencyController,
  initializeMessageStorage,
  initializeReplicationMessageListener,
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
void (async () => {
  const config = getConfig();
  const inboxConfig = getInboxConfig(config);

  // Initialize the inbox message storage to store incoming messages in the inbox
  const storeInboxMessage = initializeMessageStorage(inboxConfig, logger);

  // Initialize the RabbitMQ message handler to receive messages and store them in the inbox
  const [shutdownRmq] = await initializeRabbitMqHandler(
    config,
    inboxConfig,
    storeInboxMessage,
    [MovieCreatedMessageType],
    logger,
  );

  // Initialize and start the inbox subscription
  const storePublishedMovie = getStorePublishedMovie(logger);
  const [shutdownListener] = initializeReplicationMessageListener(
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
      concurrencyStrategy: createReplicationMutexConcurrencyController(),
      messageProcessingTimeoutStrategy: () => 2_000,
      messageProcessingTransactionLevelStrategy: () =>
        IsolationLevel.Serializable,
    },
  );

  // Close all connections
  const cleanup = async () => {
    await Promise.allSettled([shutdownRmq(), shutdownListener()]);
  };
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);
})();

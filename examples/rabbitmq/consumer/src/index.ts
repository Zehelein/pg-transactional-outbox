import * as dotenv from 'dotenv';
import path from 'path';
dotenv.config({ path: path.join(__dirname, '../.env') });
// eslint-disable-next-line prettier/prettier
import {
  IsolationLevel,
  initializeMessageStorage,
  initializePollingMessageListener,
  initializeReplicationMessageListener,
} from 'pg-transactional-outbox';
import {
  getConfig,
  getPollingInboxConfig,
  getReplicationInboxConfig,
} from './config';
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
  const replicationConfig = getReplicationInboxConfig(config);
  const pollingConfig = getPollingInboxConfig(config);

  // Initialize the inbox message storage to store incoming messages in the inbox
  const storeInboxMessage = initializeMessageStorage(replicationConfig, logger);

  // Initialize the RabbitMQ message handler to receive messages and store them in the inbox
  const [shutdownRmq] = await initializeRabbitMqHandler(
    config,
    replicationConfig,
    storeInboxMessage,
    [MovieCreatedMessageType],
    logger,
  );

  // Initialize and start the inbox subscription
  const storePublishedMovie = getStorePublishedMovie(logger);
  let shutdownListener: () => Promise<void>;
  if (process.env.listenerType === 'replication') {
    const [shutdown] = initializeReplicationMessageListener(
      replicationConfig,
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
        messageProcessingTransactionLevelStrategy: () =>
          IsolationLevel.RepeatableRead,
      },
    );
    shutdownListener = shutdown;
  } else {
    const [shutdown] = initializePollingMessageListener(
      pollingConfig,
      [
        {
          aggregateType: MovieAggregateType,
          messageType: MovieCreatedMessageType,
          handle: storePublishedMovie,
        },
      ],
      logger,
    );
    shutdownListener = shutdown;
  }

  // Close all connections
  const cleanup = async () => {
    await Promise.allSettled([shutdownRmq(), shutdownListener()]);
  };
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);
})();
function createMutexConcurrencyController():
  | import('pg-transactional-outbox').ReplicationConcurrencyController
  | undefined {
  throw new Error('Function not implemented.');
}

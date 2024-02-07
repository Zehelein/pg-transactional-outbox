import * as dotenv from 'dotenv';
import path from 'path';
dotenv.config({ path: path.join(__dirname, '../.env') });
// eslint-disable-next-line prettier/prettier
import {
  TransactionalMessage,
  createReplicationMutexConcurrencyController,
  initializePollingMessageListener,
  initializeReplicationMessageListener,
} from 'pg-transactional-outbox';
import { addMovies } from './add-movies';
import {
  getConfig,
  getPollingOutboxConfig,
  getReplicationOutboxConfig,
} from './config';
import { getLogger } from './logger';
import { initializeRabbitMqPublisher } from './rabbitmq-publisher';

// Exit the process if there is an unhandled promise error
process.on('unhandledRejection', (err, promise) => {
  getLogger().error({ err, promise }, 'Unhandled promise rejection');
  process.exit(1);
});

/** The main entry point of the message producer. */
(async () => {
  const config = getConfig();
  const replicationConfig = getReplicationOutboxConfig(config);
  const pollingConfig = getPollingOutboxConfig(config);
  const logger = getLogger();

  // Initialize the actual RabbitMQ message publisher
  const [rmqPublisher, shutdownRmq] = await initializeRabbitMqPublisher(
    config,
    logger,
  );

  // Initialize and start the outbox subscription
  let shutdownListener: () => Promise<void>;
  if (config.listenerType === 'replication') {
    const [shutdown] = initializeReplicationMessageListener(
      replicationConfig,
      {
        handle: rmqPublisher,
      },
      logger,
      {
        concurrencyStrategy: createReplicationMutexConcurrencyController(),
        messageProcessingTimeoutStrategy: (message: TransactionalMessage) =>
          message.messageType === 'ABC' ? 10_000 : 2_000,
      },
    );
    shutdownListener = shutdown;
  } else {
    const [shutdown] = initializePollingMessageListener(
      pollingConfig,
      {
        handle: rmqPublisher,
      },
      logger,
      {
        messageProcessingTimeoutStrategy: (message: TransactionalMessage) =>
          message.messageType === 'ABC' ? 10_000 : 2_000,
      },
    );
    shutdownListener = shutdown;
  }

  // Add movies and produce outbox messages on a timer
  const timeout = await addMovies(config, replicationConfig, logger);

  // Close all connections
  const cleanup = async () => {
    clearTimeout(timeout);
    await Promise.allSettled([shutdownRmq(), shutdownListener()]);
  };
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);
})();

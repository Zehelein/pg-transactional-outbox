import * as dotenv from 'dotenv';
import path from 'path';
dotenv.config({ path: path.join(__dirname, '../.env') });
// eslint-disable-next-line prettier/prettier
import {
  createMutexConcurrencyController,
  initializeOutboxListener,
} from 'pg-transactional-outbox';
import { addMovies } from './add-movies';
import { getConfig, getOutboxConfig } from './config';
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
  const outboxConfig = getOutboxConfig(config);
  const logger = getLogger();

  // Initialize the actual RabbitMQ message publisher
  const [rmqPublisher, shutdownRmq] = await initializeRabbitMqPublisher(
    config,
    logger,
  );

  // Initialize and start the outbox subscription
  const [shutdownOutSrv] = initializeOutboxListener(
    outboxConfig,
    rmqPublisher,
    logger,
    {
      concurrencyStrategy: createMutexConcurrencyController(),
      messageProcessingTimeoutStrategy: () => 1000,
    },
  );

  // Add movies and produce outbox messages on a timer
  await addMovies(config, outboxConfig, logger);

  // Close all connections
  const cleanup = async () => {
    await Promise.allSettled([shutdownRmq(), shutdownOutSrv()]);
    process.exit(0);
  };
  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);
})();

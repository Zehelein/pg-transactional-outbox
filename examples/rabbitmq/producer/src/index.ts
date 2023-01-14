import path from 'path';
import * as dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '../.env') });
import { initializeOutboxService, setLogger } from 'pg-transactional-outbox';
import { addMovies } from './add-movies';
import { getConfig, getOutboxServiceConfig } from './config';
import { logger } from './logger';
import { initializeRabbitMqPublisher } from './rabbitmq-publisher';

// Exit the process if there is an unhandled promise error
process.on('unhandledRejection', (reason, promise) => {
  logger.error({ reason, promise }, 'Unhandled promise rejection');
  process.exit(1);
});

/** The main entry point of the message producer. */
(async () => {
  // Set the pino logger also for the library logging
  setLogger(logger);
  const config = getConfig();
  const outboxConfig = getOutboxServiceConfig(config);

  // Initialize the actual RabbitMQ message publisher
  const rmqPublisher = await initializeRabbitMqPublisher(config);

  // Initialize and start the outbox subscription
  await initializeOutboxService(outboxConfig, rmqPublisher);

  // Add movies and produce outbox messages on a timer
  await addMovies(config, outboxConfig);
})();
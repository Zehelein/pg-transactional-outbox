import path from 'path';
require('dotenv').config({ path: path.join(__dirname, '../.env') });
import { getConfig } from './config';
import { initializeOutboxService } from './wal-outbox-subscription';
import { addMovies } from './add-movies';
import { initializeRabbitMqPublisher } from './rabbitmq-publisher';
import { resilienceTest } from './resilience-test';

/** The main entry point of the message producer. */
(async () => {
  const config = getConfig();

  // Initialize the actual RabbitMQ message publisher
  const rmqPublisher = await initializeRabbitMqPublisher(config);

  // Initialize and start the outbox subscription
  const outboxService = initializeOutboxService(config, rmqPublisher);

  // Add movies and produce outbox messages on a timer
  await addMovies(config);

  // Test behavior with some service outages
  resilienceTest(outboxService.stop, outboxService.startIfStopped);
})();

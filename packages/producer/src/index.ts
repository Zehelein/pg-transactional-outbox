import path from 'path';
require('dotenv').config({ path: path.join(__dirname, '../.env') });
import { getConfig } from './config';
import {
  subscribeToOutboxMessages,
  initializeOutboxService,
} from './wal-outbox-subscription';
import { addMovies } from './add-movies';
import { initializeRabbitMqPublisher } from './rabbitmq-publisher';

/** The main entry point of the message producer. */
(async () => {
  const config = getConfig();

  // Initialize the actual RabbitMQ message publisher
  const rmqPublisher = await initializeRabbitMqPublisher(config);

  // Initialize and start the outbox subscription
  const outboxService = initializeOutboxService(config, rmqPublisher);
  subscribeToOutboxMessages(outboxService);

  // Add movies and produce outbox messages on a timer
  await addMovies(config);

  // Test with some outbox service outages
  setInterval(async () => {
    if (Math.random() > 0.9) {
      // Stop service to fake some subscription outage, still produce videos
      await outboxService.service.stop();
    }
    if (Math.random() > 0.9 && outboxService.service.isStop()) {
      // Restart the subscription, it will now catch up with all outbox messages from the WAL
      subscribeToOutboxMessages(outboxService);
    }
  }, 750);
})();

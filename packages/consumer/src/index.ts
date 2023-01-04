import path from 'path';
import * as dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '../.env') });
import { logger } from './logger';
import { getConfig } from './config';
import { initializeInboxMessageStorage } from './inbox';
import { initializeRabbitMqHandler } from './rabbitmq-handler';
import {
  MovieAggregateType,
  MovieCreatedEventType,
  initializePublishedMovieStorage,
} from './receive-movie';
import { initializeInboxService } from './wal-inbox-subscription';

// Exit the process if there is an unhandled promise error
process.on('unhandledRejection', (reason, promise) => {
  logger.error({ reason, promise }, 'Unhandled promise rejection');
  process.exit(1);
});

/** The main entry point of the message producer. */
(async () => {
  const config = getConfig();

  // Initialize the inbox message storage to store incoming messages in the inbox
  const storeInboxMessage = await initializeInboxMessageStorage(config);

  // Initialize the RabbitMQ message handler to receive messages and store them in the inbox
  await initializeRabbitMqHandler(config, storeInboxMessage, [
    MovieCreatedEventType,
  ]);

  const storePublishedMovie = initializePublishedMovieStorage(config);

  // Initialize and start the inbox subscription
  initializeInboxService(config, [
    {
      aggregateType: MovieAggregateType,
      eventType: MovieCreatedEventType,
      handle: storePublishedMovie,
    },
  ]);
})();

import path from 'path';
require('dotenv').config({ path: path.join(__dirname, '../.env') });
import { getConfig } from './config';
import { initializeRabbitMqHandler } from './rabbitmq-handler';
import { MovieCreatedEventType } from './receive-movie';

// Exit the process if there is an unhandled promise error
process.on('unhandledRejection', (reason, promise) => {
  console.error(`Unhandled promise rejection: ${reason}.`, promise);
  process.exit(1);
});

/** The main entry point of the message producer. */
(async () => {
  const config = getConfig();

  // Initialize the RabbitMQ message handler to receive messages
  const rmqPublisher = await initializeRabbitMqHandler(config, [
    MovieCreatedEventType,
  ]);
})();

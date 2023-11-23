import { OutboxMessage, TransactionalLogger } from 'pg-transactional-outbox';
import { BrokerAsPromised } from 'rascal';
import { Config } from './config';
import { getMessagingConfig } from './rabbitmq-config';

/**
 * Initialize the message publisher and receive a function to publish messages.
 * @param config The configuration settings to connect to the RabbitMQ instance
 * @param logger A logger instance for logging trace up to error logs
 * @returns Returns a function to publish a message to the corresponding rascal publishing topic
 */
export const initializeRabbitMqPublisher = async (
  config: Config,
  logger: TransactionalLogger,
): Promise<
  [
    rmqPublisher: (message: OutboxMessage) => Promise<void>,
    shutdown: { (): Promise<void> },
  ]
> => {
  const cfg = getMessagingConfig(config);
  const broker = await BrokerAsPromised.create(cfg);
  broker.on('error', (err, { vhost, connectionUrl }) => {
    logger.error({ err, vhost, connectionUrl }, 'RabbitMQ broker error');
  });

  return [
    async (message: OutboxMessage): Promise<void> => {
      // Publish a message
      const publication = await broker.publish(
        message.messageType, // By convention we use the message type also as publish topic
        message, // Send the full outbox message so a receiver can use the inbox pattern to check for duplicate messages
      );
      return new Promise((resolve, reject) => {
        publication
          .on('success', (_messageId) => {
            resolve();
            logger.trace(message, 'Published outbox message');
          })
          .on('return', (_rmqMessage) => {
            logger.warn(
              message,
              `An outbox message was successfully published but was not routed.`,
            );
          })
          .on('error', (error, _messageId) => {
            reject(error);
          });
      });
    },
    broker.shutdown,
  ];
};

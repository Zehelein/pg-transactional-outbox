import { BrokerAsPromised } from 'rascal';
import { Config } from './config';
import { OutboxMessage } from './outbox';
import { getMessagingConfig } from './rabbitmq-config';

/**
 * Initialize the message publisher and receive a function to publish messages.
 * @param config The configuration settings to connect to the RabbitMQ instance
 * @returns Returns a function to publish a message to the corresponding rascal publishing topic
 */
export const initializeRabbitMqPublisher = async (
  config: Config,
): Promise<(message: OutboxMessage) => Promise<void>> => {
  const cfg = getMessagingConfig(config);
  const broker = await BrokerAsPromised.create(cfg);
  broker.on('error', (err, { vhost, connectionUrl }) => {
    console.error('Broker error', err, vhost, connectionUrl);
  });

  return async (message: OutboxMessage): Promise<void> => {
    // Publish a message
    const publication = await broker.publish(
      message.eventType, // By convention we use the event type also as publish topic
      message.payload,
    );
    return new Promise((resolve, reject) => {
      publication
        .on('success', (_messageId) => {
          resolve();
          console.log(
            `Published outbox message ${message.aggregateType}.${message.eventType}.${message.aggregateId}`,
          );
        })
        .on('return', (_rmqMessage) => {
          console.debug(
            `An outbox message ${message.aggregateType}.${message.eventType}.${message.aggregateId} was successfully published but was not routed.`,
          );
        })
        .on('error', (error, _messageId) => {
          reject(error);
        });
    });
  };
};

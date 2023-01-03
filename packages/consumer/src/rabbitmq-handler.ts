import { Message } from 'amqplib';
import { BrokerAsPromised } from 'rascal';
import { Config } from './config';
import { InboxMessage } from './inbox';
import { getMessagingConfig } from './rabbitmq-config';

/** The outbox message as it was sent by the producer */
export interface OutboxMessage {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  payload: unknown;
  createdAt: string;
}

/**
 * Initialize the message handler and receive a list of event types that should be consumed.
 * @param config The configuration settings to connect to the RabbitMQ instance
 * @param eventTypes All the event types that should be handled and be put to the inbox table.
 */
export const initializeRabbitMqHandler = async (
  config: Config,
  eventTypes: string[],
): Promise<void> => {
  const cfg = getMessagingConfig(config);
  const broker = await BrokerAsPromised.create(cfg);
  broker.on('error', (err, { vhost, connectionUrl }) => {
    console.error('Broker error', err, vhost, connectionUrl);
  });

  // Consume messages for the desired subscriptions
  eventTypes.map(async (eventType) => {
    const subscription = await broker.subscribe(eventType);
    subscription
      .on('message', (_rmqMsg, content: OutboxMessage, ackOrNack) => {
        if (content.id && content.aggregateType && content.eventType) {
          console.log(
            `Added the incoming message ${content.aggregateType}.${content.eventType}.${content.id} to the inbox.`,
          );
        }
        ackOrNack();
      })
      .on('error', console.error);
  });
};

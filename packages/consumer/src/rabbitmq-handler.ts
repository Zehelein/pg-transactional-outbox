import { BrokerAsPromised } from 'rascal';
import { Config } from './config';
import { getMessagingConfig } from './rabbitmq-config';
import { ensureError } from './utils';

/** The received message as it was sent by the producer */
export interface ReceivedMessage {
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
  storeInboxMessage: (message: ReceivedMessage) => Promise<void>,
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
      .on('message', async (_rmqMsg, content: ReceivedMessage, ackOrNack) => {
        if (
          content.id &&
          content.aggregateType &&
          content.eventType &&
          content.createdAt
        ) {
          try {
            await storeInboxMessage(content);
            console.log(
              `Added the incoming message ${content.aggregateType}.${content.eventType}.${content.aggregateId} to the inbox.`,
            );
          } catch (error) {
            console.debug(
              `Could not save the incoming message ${content.aggregateType}.${content.eventType}.${content.aggregateId} to the inbox.`,
              content,
            );
            ackOrNack(ensureError(error));
          }
        } else {
          console.debug(
            'Received a message that was not a message with the required "ReceivedMessage" fields - skipping it.',
            content,
          );
        }
        ackOrNack();
      })
      .on('error', console.error);
  });
};

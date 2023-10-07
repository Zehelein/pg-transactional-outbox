import { BrokerAsPromised } from 'rascal';
import { ensureError } from 'pg-transactional-outbox';
import { Config } from './config';
import { getMessagingConfig } from './rabbitmq-config';
import { logger } from './logger';
import { Mutex } from 'async-mutex';

/** The received message as it was sent by the producer */
export interface ReceivedMessage {
  id: string;
  aggregateType: string;
  aggregateId: string;
  messageType: string;
  payload: unknown;
  createdAt: string;
}

/**
 * Initialize the message handler and receive a list of message types that should be consumed.
 * @param config The configuration settings to connect to the RabbitMQ instance
 * @param messageTypes All the message types that should be handled and be put to the inbox table.
 */
export const initializeRabbitMqHandler = async (
  config: Config,
  storeInboxMessage: (message: ReceivedMessage) => Promise<void>,
  messageTypes: string[],
): Promise<[shutdown: { (): Promise<void> }]> => {
  const cfg = getMessagingConfig(config);
  const broker = await BrokerAsPromised.create(cfg);
  broker.on('error', (err, { vhost, connectionUrl }) => {
    logger.error({ err, vhost, connectionUrl }, 'RabbitMQ broker error');
  });
  const mutex = new Mutex();
  // Consume messages for the desired subscriptions
  messageTypes.map(async (messageType) => {
    const subscription = await broker.subscribe(messageType);
    subscription
      .on('message', async (_rmqMsg, content: ReceivedMessage, ackOrNack) => {
        if (
          content.id &&
          content.aggregateType &&
          content.messageType &&
          content.createdAt
        ) {
          // Using a mutex to ensure that each message is completely inserted
          // in the original sort order to be then also processed in this order.
          const release = await mutex.acquire();
          try {
            logger.trace(
              content,
              'Started to add the incoming message to the inbox',
            );
            await storeInboxMessage(content);
            ackOrNack();
            logger.trace(content, 'Added the incoming message to the inbox');
          } catch (error) {
            const err = ensureError(error);
            ackOrNack(err);
            logger.error(
              {
                ...content,
                err,
              },
              'Could not save the incoming message to the inbox',
            );
          } finally {
            release();
          }
        } else {
          logger.warn(
            content,
            'Received a message that was not a message with the required "ReceivedMessage" fields - skipping it',
          );
          ackOrNack();
        }
      })
      .on('error', (e) => logger.error(e, 'RabbitMQ subscription error.'))
      .on('subscribed' as any, () =>
        logger.trace('RabbitMQ subscription success.'),
      );
  });

  return [broker.shutdown];
};

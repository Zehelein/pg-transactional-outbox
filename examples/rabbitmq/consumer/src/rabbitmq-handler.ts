import { Mutex } from 'async-mutex';
import { Pool, PoolClient } from 'pg';
import {
  IsolationLevel,
  ReplicationConfig,
  TransactionalLogger,
  ensureExtendedError,
  executeTransaction,
} from 'pg-transactional-outbox';
import { BrokerAsPromised } from 'rascal';
import { Config } from './config';
import { getMessagingConfig } from './rabbitmq-config';

/** The received message as it was sent by the producer */
export interface ReceivedMessage {
  id: string;
  aggregateType: string;
  aggregateId: string;
  messageType: string;
  payload: unknown;
  metadata: Record<string, unknown>;
  createdAt: string;
}

/**
 * Initialize the message handler and receive a list of message types that should be consumed.
 * @param config The configuration settings to connect to the RabbitMQ instance.
 * @param storeInboxMessage Writes the received message into the inbox table.
 * @param messageTypes All the message types that should be handled and be put to the inbox table.
 * @param logger A logger instance for logging trace up to error logs
 */
export const initializeRabbitMqHandler = async (
  config: Config,
  inboxConfig: ReplicationConfig,
  storeInboxMessage: (
    message: ReceivedMessage,
    client: PoolClient,
  ) => Promise<void>,
  messageTypes: string[],
  logger: TransactionalLogger,
): Promise<[shutdown: { (): Promise<void> }]> => {
  const cfg = getMessagingConfig(config);

  const pool = new Pool(
    inboxConfig.dbHandlerConfig ?? inboxConfig.dbListenerConfig,
  );
  pool.on('error', (err) => {
    logger.error(ensureExtendedError(err, 'DB_ERROR'), 'PostgreSQL pool error');
  });

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
            await executeTransaction(
              await pool.connect(),
              async (client): Promise<void> => {
                await storeInboxMessage(content, client);
              },
              IsolationLevel.ReadCommitted,
            );

            ackOrNack();
            logger.trace(content, 'Added the incoming message to the inbox');
          } catch (error) {
            const err =
              error instanceof Error ? error : new Error(String(error));
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
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- rascal types don't include the subscribed event
      .on('subscribed' as any, () =>
        logger.trace('RabbitMQ subscription success.'),
      );
  });

  return [
    async () => {
      await pool.end();
      await broker.shutdown();
    },
  ];
};

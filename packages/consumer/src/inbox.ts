import { Pool, PoolClient } from 'pg';
import { Config } from './config';
import { ReceivedMessage } from './rabbitmq-handler';
import { executeTransaction } from './utils';
import { logger } from './logger';

/** The inbox message for storing it to the DB and receiving it back from the WAL */
export interface InboxMessage {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  payload: unknown;
  createdAt: string;
  retries: number;
}

const insertInbox = async (
  message: ReceivedMessage,
  dbClient: PoolClient,
  { postgresInboxSchema }: Config,
) => {
  const { id, aggregateType, aggregateId, eventType, payload, createdAt } =
    message;
  const inboxResult = await dbClient.query(
    /*sql*/ `
    INSERT INTO ${postgresInboxSchema}.inbox
      (id, aggregate_type, aggregate_id, event_type, payload, created_at)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (id) DO NOTHING`,
    [id, aggregateType, aggregateId, eventType, payload, createdAt],
  );
  if (inboxResult.rowCount < 1) {
    logger.warn(message, 'The message already existed');
  }
};

/**
 * Initialize the inbox message storage to store incoming messages in the inbox table.
 * @param config The configuration object that defines the values on how to connect to the database.
 * @returns The function to store the inbox message data to the database.
 */
export const initializeInboxMessageStorage = async (
  config: Config,
): Promise<{ (message: ReceivedMessage): Promise<void> }> => {
  const pool = new Pool({
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresLoginRole,
    password: config.postgresLoginRolePassword,
    database: config.postgresDatabase,
  });
  pool.on('error', (err) => {
    logger.error(err, 'PostgreSQL pool error');
  });

  /**
   * The function to store the inbox message data to the database.
   * @param message The received message that should be stored as inbox message
   */
  return async (message: ReceivedMessage): Promise<void> => {
    const result = await executeTransaction(pool, async (client) => {
      await insertInbox(message, client, config);
    });
    if (result instanceof Error) {
      logger.error({ ...message, result }, 'Could not store the inbox message');
      throw result;
    }
  };
};

/**
 * Acknowledge that the inbox message was handled.
 * @param message The inbox message to acknowledge.
 * @param client The database client must be part of the transaction where the business relevant data changes are done as well.
 * @param config The configuration file to pick the inbox database schema from.
 */
export const ackInbox = async (
  { id }: InboxMessage,
  client: PoolClient,
  config: Pick<Config, 'postgresInboxSchema'>,
): Promise<void> => {
  await client.query(
    /*sql*/ `UPDATE ${config.postgresInboxSchema}.inbox SET processed_at = $1 WHERE id = $2`,
    [new Date().toISOString(), id],
  );
};

/**
 * Reject the message and do not acknowledge it. This will throw an error and thus retry to handle the message later.
 * When the maximum number of retries is reached it will not throw an error and the message will be removed from the WAL.
 * @param message The inbox message to NOT acknowledge.
 * @param client The database client must be part of the transaction where the business relevant data changes are done as well.
 * @param config The configuration file to pick the inbox database schema from.
 */
export const nackInbox = async (
  message: InboxMessage,
  client: PoolClient,
  config: Pick<Config, 'postgresInboxSchema'>,
  maxRetries = 5,
): Promise<void> => {
  const { id, aggregateType, eventType, aggregateId } = message;
  const response = await client.query(
    /*sql*/ `
    UPDATE ${config.postgresInboxSchema}.inbox SET retries = retries + 1 WHERE id = $1
    RETURNING retries;
    `,
    [id],
  );
  if (response.rowCount > 0 && response.rows[0].retries < maxRetries) {
    throw new Error(
      `There was an error when trying to handle the inbox message ${aggregateType}.${eventType}.${aggregateId} with the message ID ${id}.`,
    );
  } else {
    // Not throwing an error will mark the WAL inbox message as finished
    logger.error(
      message,
      `Retries for the inbox message exceeded the maximum number of ${maxRetries} retries`,
    );
  }
};

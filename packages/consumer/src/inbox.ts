import { Pool, PoolClient } from 'pg';
import { Config } from './config';
import { ReceivedMessage } from './rabbitmq-handler';
import { ensureError, executeTransaction } from './utils';

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
  {
    id,
    aggregateType,
    aggregateId,
    eventType,
    payload,
    createdAt,
  }: ReceivedMessage,
  dbClient: PoolClient,
  { postgresInboxSchema }: Config,
) => {
  const inboxResult = await dbClient.query(
    /*sql*/ `
    INSERT INTO ${postgresInboxSchema}.inbox
      (id, aggregate_type, aggregate_id, event_type, payload, created_at)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (id) DO NOTHING`,
    [id, aggregateType, aggregateId, eventType, payload, createdAt],
  );
  if (inboxResult.rowCount < 1) {
    console.debug(
      `The message ${aggregateType}.${eventType}.${aggregateId} with message ID ${id} already existed.`,
    );
  }
};

/**
 * Initialize the inbox message storage to store incoming messages in the inbox table.
 * @param config The configuration object that defines the values on how to connect to the database.
 * @returns The function to store the inbox message data to the database.
 */
export const initializeInboxMessageStorage = async (config: Config) => {
  const pool = new Pool({
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresLoginRole,
    password: config.postgresLoginRolePassword,
    database: config.postgresDatabase,
  });
  pool.on('error', (err) => {
    console.log('Error pool', err.message);
  });

  /**
   * The function to store the inbox message data to the database.
   * @param message The received message that should be stored as inbox message
   */
  return async (message: ReceivedMessage) => {
    const result = await executeTransaction(pool, async (client) => {
      await insertInbox(message, client, config);
    });
    if (result instanceof Error) {
      console.error('Could not store the inbox message', result);
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
  { id, aggregateType, eventType, aggregateId }: InboxMessage,
  client: PoolClient,
  config: Pick<Config, 'postgresInboxSchema'>,
  maxRetries: number = 5,
): Promise<void> => {
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
    console.log(
      `Retries for the inbox message ${aggregateType}.${eventType}.${aggregateId} with message ID ${id} exceeded the maximum number of ${maxRetries} retries.`,
    );
  }
};

/**
 * Maps the database inbox message record to the inbox message type.
 * @param inboxMessage The inbox message as it was written to the database.
 */
export const mapInbox = (inboxMessage: Record<string, any>): InboxMessage => {
  return {
    id: inboxMessage.id,
    aggregateType: inboxMessage.aggregate_type,
    aggregateId: inboxMessage.aggregate_id,
    eventType: inboxMessage.event_type,
    payload: inboxMessage.payload,
    createdAt: inboxMessage.created_at,
    retries: inboxMessage.retries,
  };
};

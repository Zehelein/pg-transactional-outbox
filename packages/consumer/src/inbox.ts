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

/**
 * An inbox related error. The code contains the reason on which issue ocurred.
 */
export class InboxError extends Error {
  code: InboxErrorType;

  constructor(message: string, code: InboxErrorType) {
    super(message);
    this.code = code;
  }
}

type InboxErrorType =
  | 'INBOX_MESSAGE_NOT_FOUND'
  | 'ALREADY_PROCESSED'
  | 'MESSAGE_HANDLING_ERROR'
  | 'STORE_INBOX_MESSAGE_FAILED';

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
   * @throws InboxError if the inbox message could not be stored
   */
  return async (message: ReceivedMessage): Promise<void> => {
    try {
      await executeTransaction(pool, async (client) => {
        await insertInbox(message, client, config);
      });
    } catch (err) {
      logger.error({ ...message, err }, 'Could not store the inbox message');
      throw new InboxError(
        `Could not store the inbox message with id ${message.id}`,
        'STORE_INBOX_MESSAGE_FAILED',
      );
    }
  };
};

/**
 * Make sure the inbox item was not and is not currently being worked on.
 * As the WAL inbox service does not run in the same transaction as the message
 * handler code there is a small chance that the handler code succeeds but the
 * WAL inbox message was not acknowledged. This takes care of such cases.
 * @param message The inbox message to check and later process.
 * @param client The database client must be part of the transaction where the business relevant data changes are done as well.
 * @param config The configuration file to pick the inbox database schema from.
 * @throws Throws an error if the database row does not exist, is locked by some other transaction, or was already processed.
 */
export const verifyInbox = async (
  { id }: InboxMessage,
  client: PoolClient,
  config: Pick<Config, 'postgresInboxSchema'>,
): Promise<void> => {
  // Get the inbox data and lock it for updates. Use NOWAIT to immediately fail if another process is locking it.
  const inboxResult = await client.query(
    /*sql*/ `SELECT processed_at FROM ${config.postgresInboxSchema}.inbox WHERE id = $1 FOR UPDATE NOWAIT`,
    [id],
  );
  if (inboxResult.rowCount === 0) {
    throw new InboxError(
      `Received a WAL inbox message with id ${id} but found no database entry.`,
      'INBOX_MESSAGE_NOT_FOUND',
    );
  }
  if (inboxResult.rows[0].processed_at) {
    throw new InboxError(
      `The inbox message ${id} was already processed.`,
      'ALREADY_PROCESSED',
    );
  }
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
 * Reject the message and do not acknowledge it. Check the return value if the
 * WAL message should be acknowledged or not.
 * @param message The inbox message to NOT acknowledge.
 * @param client The database client must be part of the transaction where the business relevant data changes are done as well.
 * @param config The configuration file to pick the inbox database schema from.
 * @returns "RETRY" if the maximum number of retries was not reached - otherwise "RETRIES_EXCEEDED"
 */
export const nackInbox = async (
  message: InboxMessage,
  client: PoolClient,
  config: Pick<Config, 'postgresInboxSchema'>,
  maxRetries = 5,
): Promise<'RETRY' | 'RETRIES_EXCEEDED'> => {
  const response = await client.query(
    /*sql*/ `
    UPDATE ${config.postgresInboxSchema}.inbox SET retries = retries + 1 WHERE id = $1
    RETURNING retries;
    `,
    [message.id],
  );
  if (response.rowCount > 0 && response.rows[0].retries < maxRetries) {
    logger.warn(message, 'Increased the retry counter on the inbox message.');
    return 'RETRY';
  } else {
    logger.error(
      message,
      `Retries for the inbox message exceeded the maximum number of ${maxRetries} retries`,
    );
    return 'RETRIES_EXCEEDED';
  }
};

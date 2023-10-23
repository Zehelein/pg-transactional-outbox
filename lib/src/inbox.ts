import { Pool, PoolClient } from 'pg';
import { executeTransaction } from './utils';
import { logger } from './logger';
import { InboxServiceConfig } from './inbox-service';
import { MessageError, OutboxMessage, InboxMessage } from './models';

/**
 * Initialize the inbox message storage to store incoming messages in the inbox table.
 * @param config The configuration object that defines the values on how to connect to the database and general settings.
 * @returns The function to store the inbox message data to the database and the shutdown action.
 */
export const initializeInboxMessageStorage = (
  config: Pick<InboxServiceConfig, 'pgConfig' | 'settings'>,
): [
  storeInboxMessage: (message: OutboxMessage) => Promise<void>,
  shutdown: () => Promise<void>,
] => {
  const pool = new Pool(config.pgConfig);
  pool.on('error', (err) => {
    logger().error(err, 'PostgreSQL pool error');
  });

  /**
   * The function to store the inbox message data to the database.
   * @param message The received message that should be stored as inbox message
   * @throws Error if the inbox message could not be stored
   */
  return [
    async (message: OutboxMessage): Promise<void> => {
      try {
        await executeTransaction(pool, async (client) => {
          await insertInbox(message, client, config);
        });
      } catch (err) {
        logger().error(
          { ...message, err },
          'Could not store the inbox message',
        );
        throw new MessageError(
          `Could not store the inbox message with id ${message.id}`,
          message,
        );
      }
    },
    async () => {
      pool.removeAllListeners();
      try {
        await pool.end();
      } catch (e) {
        logger().error(e, 'Inbox message storage shutdown error');
      }
    },
  ];
};

/**
 * Make sure the inbox item was not and is not currently being worked on.
 * As the WAL inbox service does not run in the same transaction as the message
 * handler code there is a small chance that the handler code succeeds but the
 * WAL inbox message was not acknowledged. This takes care of such cases.
 * @param message The inbox message to check and later process.
 * @param client The database client. Must be part of the transaction where the message handling changes are done.
 * @param config The configuration settings that defines inbox database schema.
 * @throws Throws an error if the database row does not exist, is locked by some other transaction, or was already processed.
 */
export const verifyInbox = async (
  { id }: InboxMessage,
  client: PoolClient,
  { settings }: Pick<InboxServiceConfig, 'settings'>,
): Promise<
  | true
  | 'INBOX_MESSAGE_NOT_FOUND'
  | 'ALREADY_PROCESSED'
  | 'MAX_ATTEMPTS_EXCEEDED'
> => {
  // Get the inbox data and lock it for updates. Use NOWAIT to immediately fail if another process is locking it.
  const inboxResult = await client.query(
    /* sql*/ `SELECT processed_at, attempts FROM ${settings.dbSchema}.${settings.dbTable} WHERE id = $1 FOR UPDATE NOWAIT`,
    [id],
  );
  if (inboxResult.rowCount === 0) {
    return 'INBOX_MESSAGE_NOT_FOUND';
  }
  if (inboxResult.rows[0].processed_at) {
    return 'ALREADY_PROCESSED';
  }
  if (inboxResult.rows[0].attempts >= getMaxAttempts(settings.maxAttempts)) {
    return 'MAX_ATTEMPTS_EXCEEDED';
  }
  return true;
};

/**
 * Acknowledge that the inbox message was handled.
 * @param message The inbox message to acknowledge.
 * @param client The database client. Must be part of the transaction where the message handling changes are done.
 * @param config The configuration settings that defines inbox database schema.
 */
export const ackInbox = async (
  { id }: InboxMessage,
  client: PoolClient,
  { settings }: Pick<InboxServiceConfig, 'settings'>,
): Promise<void> => {
  await client.query(
    /* sql*/ `UPDATE ${settings.dbSchema}.${settings.dbTable} SET processed_at = $1, attempts = attempts + 1 WHERE id = $2`,
    [new Date().toISOString(), id],
  );
};

/**
 * Reject the message and do not acknowledge it. Check the return value if the
 * WAL message should be acknowledged or not as the logic uses retries.
 * @param message The inbox message to NOT acknowledge.
 * @param client The database client. Must be part of the transaction where the message handling changes are done.
 * @param config The configuration settings that defines inbox database schema.
 * @param attempts Optionally set the number of attempts to a specific value. Especially relevant for non-transient errors to directly set it to the maximum attempts value.
 */
export const nackInbox = async (
  { id }: InboxMessage,
  client: PoolClient,
  { settings }: Pick<InboxServiceConfig, 'settings'>,
  attempts?: number,
): Promise<void> => {
  if (attempts) {
    await client.query(
      /* sql*/ `
      UPDATE ${settings.dbSchema}.${settings.dbTable} SET attempts = $1 WHERE id = $2;`,
      [attempts, id],
    );
  } else {
    await client.query(
      /* sql*/ `
    UPDATE ${settings.dbSchema}.${settings.dbTable} SET attempts = attempts + 1 WHERE id = $1;`,
      [id],
    );
  }
};

export const getMaxAttempts = (maxAttempts?: number): number =>
  maxAttempts ?? 5;

const insertInbox = async (
  message: OutboxMessage,
  dbClient: PoolClient,
  { settings }: Pick<InboxServiceConfig, 'settings'>,
) => {
  const {
    id,
    aggregateType,
    aggregateId,
    messageType,
    payload,
    metadata,
    createdAt,
  } = message;
  const inboxResult = await dbClient.query(
    /* sql*/ `
    INSERT INTO ${settings.dbSchema}.${settings.dbTable}
      (id, aggregate_type, aggregate_id, message_type, payload, metadata, created_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (id) DO NOTHING`,
    [id, aggregateType, aggregateId, messageType, payload, metadata, createdAt],
  );
  if (inboxResult.rowCount < 1) {
    logger().warn(message, `The message with id ${id} already existed`);
  }
};

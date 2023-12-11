import { Pool, PoolClient } from 'pg';
import { MessageError, ensureError } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { InboxMessage, OutboxMessage } from '../common/message';
import { IsolationLevel, executeTransaction } from '../common/utils';
import { InboxConfig } from './inbox-listener';

/**
 * Initialize the inbox message storage to store incoming messages in the inbox table.
 * @param config The configuration object that defines the values on how to connect to the database and general settings.
 * @param logger A logger instance for logging trace up to error logs
 * @returns The function to store the inbox message data to the database and the shutdown action.
 */
export const initializeInboxMessageStorage = (
  config: Pick<InboxConfig, 'pgConfig' | 'settings'>,
  logger: TransactionalLogger,
): [
  storeInboxMessage: (message: OutboxMessage) => Promise<void>,
  shutdown: () => Promise<void>,
] => {
  const pool = new Pool(config.pgConfig);
  pool.on('error', (err) => {
    logger.error(err, 'PostgreSQL pool error');
  });

  /**
   * The function to store the inbox message data to the database.
   * @param message The received message that should be stored as inbox message
   * @throws Error if the inbox message could not be stored
   */
  return [
    async (message: OutboxMessage): Promise<void> => {
      try {
        await executeTransaction(
          pool,
          async (client) => {
            await insertInbox(message, client, config, logger);
          },
          IsolationLevel.ReadCommitted,
          logger,
        );
      } catch (err) {
        logger.error({ ...message, err }, 'Could not store the inbox message');
        throw new MessageError(
          `Could not store the inbox message with id ${message.id}`,
          message,
          ensureError(err),
        );
      }
    },
    async () => {
      pool.removeAllListeners();
      try {
        await pool.end();
      } catch (e) {
        logger.error(e, 'Inbox message storage shutdown error');
      }
    },
  ];
};

/**
 * This function increases the "started_attempts" for the inbox message by one
 * in the inbox table. This number can then be compared to the
 * "finished_attempts" number which is only increased when a message processing
 * exception was correctly handled or an error was caught. A difference between
 * the two can only happen if the service crashes after increasing the
 * "started_attempts" but before successfully marking the message as done
 * (success case) or catching an error (error case). If the "started_attempts"
 * and the "finished_attempts" field differ by more than one, the chances are
 * high that this message is causing a service crash.
 * @param message The inbox message for which to check/increase the "started_attempts".
 * @param client The database client. Must be part of the transaction where the message handling changes are done.
 * @param config The configuration settings that defines inbox database schema and poisonous settings.
 * @returns The "started_attempts" (one or higher) and the "finished_attempts" (zero or higher). Or undefined if the message was not found.
 */
export const poisonousMessageUpdate = async (
  { id }: InboxMessage,
  client: PoolClient,
  { settings }: Pick<InboxConfig, 'settings'>,
): Promise<PoisonousCheck | undefined> => {
  const inboxResult = await client.query(
    /* sql*/ `
      UPDATE ${settings.dbSchema}.${settings.dbTable} SET started_attempts = started_attempts + 1 WHERE id = $1
        RETURNING started_attempts, finished_attempts;`,
    [id],
  );
  if (inboxResult.rowCount === 0) {
    return undefined;
  }
  const dbItem = inboxResult.rows[0];
  return {
    startedAttempts: dbItem.started_attempts,
    finishedAttempts: dbItem.finished_attempts,
  };
};

export interface PoisonousCheck {
  startedAttempts: number;
  finishedAttempts: number;
}

/**
 * Make sure the inbox item was not and is not currently being worked on. And
 * set the started_attempts, finished_attempts and processed_at values for the
 * WAL message. As the inbox listener does not run in the same transaction as
 * the message handler code there is a small chance that the handler code
 * succeeds but the WAL inbox message was not acknowledged. This takes care of
 * such cases by ensuring that the message was not yet processed and does not
 * have too many attempts.
 * @param message The inbox message to check and later process.
 * @param client The database client. Must be part of the transaction where the message handling changes are done.
 * @param config The configuration settings that defines inbox database schema.
 * @throws Throws an error if the database row does not exist, is locked by some other transaction, or was already processed.
 */
export const verifyInbox = async (
  message: InboxMessage,
  client: PoolClient,
  { settings }: Pick<InboxConfig, 'settings'>,
): Promise<
  | true
  | 'INBOX_MESSAGE_NOT_FOUND'
  | 'ALREADY_PROCESSED'
  | 'MAX_ATTEMPTS_EXCEEDED'
> => {
  // Get the inbox data and lock it for updates. Use NOWAIT to immediately fail if another process is locking it.
  const inboxResult = await client.query(
    /* sql*/ `SELECT started_attempts, finished_attempts, processed_at FROM ${settings.dbSchema}.${settings.dbTable} WHERE id = $1 FOR UPDATE NOWAIT`,
    [message.id],
  );
  if (inboxResult.rowCount === 0) {
    return 'INBOX_MESSAGE_NOT_FOUND';
  }
  const dbItem = inboxResult.rows[0];
  if (dbItem.processed_at) {
    return 'ALREADY_PROCESSED';
  }
  if (dbItem.finished_attempts >= getMaxAttempts(settings.maxAttempts)) {
    return 'MAX_ATTEMPTS_EXCEEDED';
  }
  // assign the started_attempts, finished_attempts, and processed_at to the WAL message
  message.startedAttempts = dbItem.started_attempts;
  message.finishedAttempts = dbItem.finished_attempts;
  message.processedAt = dbItem.processed_at;
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
  { settings }: Pick<InboxConfig, 'settings'>,
): Promise<void> => {
  await client.query(
    /* sql*/ `UPDATE ${settings.dbSchema}.${settings.dbTable} SET processed_at = $1, finished_attempts = finished_attempts + 1 WHERE id = $2`,
    [new Date().toISOString(), id],
  );
};

/**
 * Reject the message and do not acknowledge it. Check the return value if the
 * WAL message should be acknowledged or not as the logic uses retries.
 * @param message The inbox message to NOT acknowledge.
 * @param client The database client. Must be part of the transaction where the message handling changes are done.
 * @param config The configuration settings that defines inbox database schema.
 * @param finishedAttempts Optionally set the number of finished_attempts to a specific value. Especially relevant for non-transient errors to directly set it to the maximum attempts value.
 */
export const nackInbox = async (
  { id }: InboxMessage,
  client: PoolClient,
  { settings }: Pick<InboxConfig, 'settings'>,
  finishedAttempts?: number,
): Promise<void> => {
  if (finishedAttempts) {
    await client.query(
      /* sql*/ `
      UPDATE ${settings.dbSchema}.${settings.dbTable} SET finished_attempts = $1 WHERE id = $2;`,
      [finishedAttempts, id],
    );
  } else {
    await client.query(
      /* sql*/ `
    UPDATE ${settings.dbSchema}.${settings.dbTable} SET finished_attempts = finished_attempts + 1 WHERE id = $1;`,
      [id],
    );
  }
};

/** Gets the maximum attempts to process a message. Defaults to 5 if not configured. */
export const getMaxAttempts = (maxAttempts?: number): number =>
  maxAttempts ?? 5;

const insertInbox = async (
  message: OutboxMessage,
  dbClient: PoolClient,
  { settings }: Pick<InboxConfig, 'settings'>,
  logger: TransactionalLogger,
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
  if (!inboxResult.rowCount || inboxResult.rowCount < 1) {
    logger.warn(message, `The message with id ${id} already existed`);
  }
};

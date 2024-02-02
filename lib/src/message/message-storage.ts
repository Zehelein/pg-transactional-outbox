import { PoolClient } from 'pg';
import { ListenerConfig } from '../common/base-config';
import { MessageError } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { StoredTransactionalMessage, TransactionalMessage } from './message';

/**
 * Initialize the message storage to store outbox or inbox messages in the table.
 * @param config The configuration object that defines the values on how to connect to the database and general settings.
 * @param logger A logger instance for logging trace up to error logs
 * @returns Initializes the function to store the outbox or inbox message data to the database and provides the shutdown action.
 */
export const initializeMessageStorage = (
  config: ListenerConfig,
  logger: TransactionalLogger,
): ((message: TransactionalMessage, client: PoolClient) => Promise<void>) => {
  /**
   * The function to store the message data to the database.
   * @param message The received message that should be stored as a outbox or inbox message
   * @param client A database client with an active transaction(!) can be provided. Otherwise
   * @throws Error if the message could not be stored
   */
  return async (
    message: TransactionalMessage,
    client: PoolClient,
  ): Promise<void> => {
    try {
      await insertMessage(message, client, config, logger);
    } catch (err) {
      const messageError = new MessageError(
        `Could not store the ${config.outboxOrInbox} message with id ${message.id}`,
        'MESSAGE_STORAGE_FAILED',
        message,
        err,
      );
      logger.error(
        messageError,
        `Could not store the ${config.outboxOrInbox} message`,
      );
      throw messageError;
    }
  };
};

/**
 * This function increases the "started_attempts" for the outbox or inbox
 * message by one in the table. This number can then be compared to the
 * "finished_attempts" number which is only increased when a message processing
 * exception was correctly handled or an error was caught. A difference between
 * the two can only happen if the service crashes after increasing the
 * "started_attempts" but before successfully marking the message as done
 * (success case) or catching an error (error case). If the "started_attempts"
 * and the "finished_attempts" field differ by more than one, the chances are
 * high that this message is causing a service crash.
 * It sets the started_attempts, finished_attempts and processed_at
 * values on the message.
 * For additional safety it makes sure, that the message was not and is not
 * currently being worked on.
 * @param message The message for which to acquire a lock and increment the started_attempts
 * @param client The database client. Must be part of a transaction that runs before the message handling transaction.
 * @param config The configuration settings that defines the database schema.
 * @returns 'MESSAGE_NOT_FOUND' if the message was not found, 'ALREADY_PROCESSED' if it was processed, and otherwise assigns the properties to the message and returns true.
 */
export const startedAttemptsIncrement = async (
  message: StoredTransactionalMessage,
  client: PoolClient,
  { settings }: Pick<ListenerConfig, 'settings'>,
): Promise<true | 'MESSAGE_NOT_FOUND' | 'ALREADY_PROCESSED'> => {
  // Use a NOWAIT select to fully lock and immediately fail if another process is locking that message row
  const updateResult = await client.query(
    /* sql*/ `
      UPDATE ${settings.dbSchema}.${settings.dbTable} SET started_attempts = started_attempts + 1 WHERE id IN
        (SELECT id FROM ${settings.dbSchema}.${settings.dbTable} WHERE id = $1 FOR UPDATE NOWAIT)
        RETURNING started_attempts, finished_attempts, processed_at;`,
    [message.id],
  );
  if (updateResult.rowCount === 0) {
    return 'MESSAGE_NOT_FOUND';
  }
  const { started_attempts, finished_attempts, processed_at } =
    updateResult.rows[0];
  if (processed_at) {
    return 'ALREADY_PROCESSED';
  }
  // set the values for the poisonous message strategy
  message.startedAttempts = started_attempts;
  message.finishedAttempts = finished_attempts;
  message.processedAt = processed_at;
  return true;
};

/**
 * This function makes sure the message was not and is not currently being
 * worked on and acquires a lock to prevent other processes to work with this
 * message. As the listener does not run in the same transaction as the message
 * handler code there is a small chance that the handler code succeeds but the
 * WAL message was not acknowledged. This takes care of such cases by ensuring
 * that the message was not yet processed. It sets the started_attempts,
 * finished_attempts and processed_at values (again) on the message to be sure
 * no other process altered them.
 * @param message The message for which to acquire a lock and fill the started_attempts, finished_attempts and processed_at values
 * @param client The database client. Must be part of the transaction where the message handling changes are later done.
 * @param config The configuration settings that defines database schema.
 * @returns 'MESSAGE_NOT_FOUND' if the message was not found, 'ALREADY_PROCESSED' if it was processed, and otherwise assigns the properties to the message and returns true.
 */
export const initiateMessageProcessing = async (
  message: StoredTransactionalMessage,
  client: PoolClient,
  { settings }: Pick<ListenerConfig, 'settings'>,
): Promise<true | 'MESSAGE_NOT_FOUND' | 'ALREADY_PROCESSED'> => {
  // Use a NOWAIT select to immediately fail if another process is locking that message
  const selectResult = await client.query(
    /* sql*/ `
    SELECT started_attempts, finished_attempts, processed_at FROM ${settings.dbSchema}.${settings.dbTable} WHERE id = $1 FOR UPDATE NOWAIT;`,
    [message.id],
  );
  if (selectResult.rowCount === 0) {
    return 'MESSAGE_NOT_FOUND';
  }
  const { started_attempts, finished_attempts, processed_at } =
    selectResult.rows[0];
  // ensures latest values (e.g. if the `startedAttemptsIncrement` was not called or another
  // process changed them between the `startedAttemptsIncrement` and this call.
  message.startedAttempts = started_attempts;
  message.finishedAttempts = finished_attempts;
  message.processedAt = processed_at;
  if (processed_at) {
    return 'ALREADY_PROCESSED';
  }
  return true;
};

/**
 * Marks the message as handled by setting the processed_at date to the current date and time.
 * It also increases the finished_attempts count by one.
 * @param message The message to acknowledge.
 * @param client The database client. Must be part of the transaction where the message handling changes are done.
 * @param config The configuration settings that defines database schema.
 */
export const markMessageCompleted = async (
  { id }: StoredTransactionalMessage,
  client: PoolClient,
  { settings }: Pick<ListenerConfig, 'settings'>,
): Promise<void> => {
  await client.query(
    /* sql*/ `UPDATE ${settings.dbSchema}.${settings.dbTable} SET processed_at = $1, finished_attempts = finished_attempts + 1 WHERE id = $2`,
    [new Date().toISOString(), id],
  );
};

/**
 * Does increase the finished_attempts count by one.
 * @param message The message to NOT acknowledge.
 * @param client The database client. Must be part of the transaction where the message handling changes are done.
 * @param config The configuration settings that defines the database schema.
 */
export const increaseMessageFinishedAttempts = async (
  { id }: StoredTransactionalMessage,
  client: PoolClient,
  { settings }: Pick<ListenerConfig, 'settings'>,
): Promise<void> => {
  await client.query(
    /* sql*/ `
    UPDATE ${settings.dbSchema}.${settings.dbTable} SET finished_attempts = finished_attempts + 1 WHERE id = $1;`,
    [id],
  );
};

const insertMessage = async (
  message: TransactionalMessage,
  client: PoolClient,
  { settings }: Pick<ListenerConfig, 'settings'>,
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
  const messageResult = await client.query(
    /* sql*/ `
    INSERT INTO ${settings.dbSchema}.${settings.dbTable}
      (id, aggregate_type, aggregate_id, message_type, payload, metadata, created_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (id) DO NOTHING`,
    [id, aggregateType, aggregateId, messageType, payload, metadata, createdAt],
  );
  if (!messageResult.rowCount || messageResult.rowCount < 1) {
    logger.warn(message, `The message with id ${id} already existed`);
  }
};

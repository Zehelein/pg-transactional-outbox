import { ListenerConfig } from '../common/base-config';
import { DatabaseClient } from '../common/database';
import { StoredTransactionalMessage } from './transactional-message';

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
 * It sets the started_attempts, finished_attempts, locked_until. abandoned_at,
 * and processed_at values on the message.
 * For additional safety it makes sure, that the message was not and is not
 * currently being worked on.
 * @param message The message for which to acquire a lock and increment the started_attempts
 * @param client The database client. Must be part of a transaction that runs before the message handling transaction.
 * @param config The configuration settings that defines the database schema.
 * @returns 'MESSAGE_NOT_FOUND' if the message was not found, 'ALREADY_PROCESSED' if it was processed, and otherwise assigns the properties to the message and returns true.
 */
export const startedAttemptsIncrement = async (
  message: StoredTransactionalMessage,
  client: DatabaseClient,
  { settings }: Pick<ListenerConfig, 'settings'>,
): Promise<
  true | 'MESSAGE_NOT_FOUND' | 'ALREADY_PROCESSED' | 'ABANDONED_MESSAGE'
> => {
  // Use a NOWAIT select to fully lock and immediately fail if another process is locking that message row
  const updateResult = await client.query(
    /* sql */ `
      UPDATE ${settings.dbSchema}.${settings.dbTable} SET started_attempts = started_attempts + 1 WHERE id IN
        (SELECT id FROM ${settings.dbSchema}.${settings.dbTable} WHERE id = $1 FOR UPDATE NOWAIT)
        RETURNING started_attempts, finished_attempts, locked_until, processed_at, abandoned_at;`,
    [message.id],
  );
  if (updateResult.rowCount === 0) {
    return 'MESSAGE_NOT_FOUND';
  }
  const {
    started_attempts,
    finished_attempts,
    locked_until,
    processed_at,
    abandoned_at,
  } = updateResult.rows[0];
  if (processed_at) {
    return 'ALREADY_PROCESSED';
  }
  if (abandoned_at) {
    return 'ABANDONED_MESSAGE';
  }
  // set the values for the poisonous message strategy
  message.startedAttempts = started_attempts;
  message.finishedAttempts = finished_attempts;
  message.lockedUntil = locked_until?.toISOString() ?? null;
  // If we get here the message is neither processed nor abandoned
  message.processedAt = null;
  message.abandonedAt = null;
  return true;
};

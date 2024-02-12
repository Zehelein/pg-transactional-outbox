import { DatabaseClient } from '../common/database';
import { PollingListenerConfig } from '../polling/config';
import { ReplicationListenerConfig } from '../replication/config';
import { StoredTransactionalMessage } from './transactional-message';

/**
 * This function makes sure the message was not and is not currently being
 * worked on and acquires a lock to prevent other processes to work with this
 * message. It locks the record via `SELECT ... FOR NO KEY UPDATE`. It updates
 * the message object and sets the started_attempts, finished_attempts,
 * locked_until, and processed_at values (again) on the message to be sure no
 * other process altered them.
 * @param message The message for which to acquire a lock and set the updatable properties (again).
 * @param client The database client. Must be part of the transaction where the message handling changes are later done.
 * @param config The configuration settings for the polling or replication listener.
 * @returns 'MESSAGE_NOT_FOUND' if the message was not found, 'ALREADY_PROCESSED' if it was processed, and otherwise assigns the properties to the message and returns it.
 */
export const initiateMessageProcessing = async (
  message: StoredTransactionalMessage,
  client: DatabaseClient,
  settings: PollingListenerConfig | ReplicationListenerConfig,
): Promise<
  true | 'MESSAGE_NOT_FOUND' | 'ALREADY_PROCESSED' | 'ABANDONED_MESSAGE'
> => {
  // Use a NOWAIT select to immediately fail if another process is locking that message
  const selectResult = await client.query(
    /* sql */ `
    SELECT started_attempts, finished_attempts, processed_at, abandoned_at, locked_until FROM ${settings.dbSchema}.${settings.dbTable} WHERE id = $1 FOR NO KEY UPDATE NOWAIT;`,
    [message.id],
  );
  if (selectResult.rowCount === 0) {
    return 'MESSAGE_NOT_FOUND';
  }
  const {
    started_attempts,
    finished_attempts,
    processed_at,
    abandoned_at,
    locked_until,
  } = selectResult.rows[0];
  // ensures latest values (e.g. if the `startedAttemptsIncrement` was not called or another
  // process changed them between the `startedAttemptsIncrement` and this call.
  message.startedAttempts = started_attempts;
  message.finishedAttempts = finished_attempts;
  message.lockedUntil = locked_until?.toISOString();
  message.processedAt = processed_at?.toISOString() ?? null;
  message.abandonedAt = abandoned_at?.toISOString() ?? null;
  if (processed_at) {
    return 'ALREADY_PROCESSED';
  }
  if (abandoned_at) {
    return 'ABANDONED_MESSAGE';
  }
  return true;
};

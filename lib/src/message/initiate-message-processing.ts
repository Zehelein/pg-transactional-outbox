import { QueryResult } from 'pg';
import { DatabaseClient } from '../common/database';
import { sleep } from '../common/utils';
import { PollingListenerSettings } from '../polling/config';
import { ReplicationListenerSettings } from '../replication/config';
import { MessageNotFoundRetryStrategy } from '../strategies/message-not-found-retry-strategy';
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
 * @param messageNotFoundRetryStrategy The retry strategy if the message could not be found in the database.
 * @returns 'MESSAGE_NOT_FOUND' if the message was not found, 'ALREADY_PROCESSED' if it was processed, and otherwise assigns the properties to the message and returns it.
 */
export const initiateMessageProcessing = async (
  message: StoredTransactionalMessage,
  client: DatabaseClient,
  settings: PollingListenerSettings | ReplicationListenerSettings,
  messageNotFoundRetryStrategy: MessageNotFoundRetryStrategy,
): Promise<
  true | 'MESSAGE_NOT_FOUND' | 'ALREADY_PROCESSED' | 'ABANDONED_MESSAGE'
> => {
  const selectResult = await loadAndLockMessage(
    message,
    client,
    settings,
    messageNotFoundRetryStrategy,
  );
  if (selectResult === 'MESSAGE_NOT_FOUND') {
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

/**
 * Load and lock (and potentially retry) a message.
 * @param message The message for which to acquire the lock.
 * @param client The database client. Must be part of the transaction where the message handling changes are later done.
 * @param config The configuration settings for the polling or replication listener.
 * @param messageNotFoundRetryStrategy The retry strategy if the message could not be found in the database.
 * @returns 'MESSAGE_NOT_FOUND' if the message was not found, otherwise the message details from the database.
 */
const loadAndLockMessage = async (
  message: StoredTransactionalMessage,
  client: DatabaseClient,
  settings: PollingListenerSettings | ReplicationListenerSettings,
  messageNotFoundRetryStrategy: MessageNotFoundRetryStrategy,
): Promise<QueryResult<any> | 'MESSAGE_NOT_FOUND'> => {
  let selectResult: QueryResult<any>;
  let attempts = 0;
  do {
    // Use a NOWAIT select to immediately fail if another process is locking that message
    selectResult = await client.query(
      /* sql */ `
      SELECT started_attempts, finished_attempts, processed_at, abandoned_at, locked_until FROM ${settings.dbSchema}.${settings.dbTable} WHERE id = $1 FOR NO KEY UPDATE NOWAIT;`,
      [message.id],
    );
    if (selectResult.rowCount === 0 || selectResult.rowCount === null) {
      const { retry, delayInMs } = messageNotFoundRetryStrategy(
        message,
        ++attempts,
      );
      if (retry) {
        await sleep(delayInMs);
      } else {
        return 'MESSAGE_NOT_FOUND';
      }
    } else {
      return selectResult;
    }
    // eslint-disable-next-line no-constant-condition
  } while (true);
};

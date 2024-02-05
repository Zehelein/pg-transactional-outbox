import { PoolClient } from 'pg';
import { ListenerConfig } from '../common/base-config';
import { StoredTransactionalMessage } from './transactional-message';

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

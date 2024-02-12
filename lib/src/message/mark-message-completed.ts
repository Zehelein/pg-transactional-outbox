import { ListenerConfig } from '../common/base-config';
import { DatabaseClient } from '../common/database';
import { StoredTransactionalMessage } from './transactional-message';

/**
 * Marks the message as handled by setting the processed_at date to the current date and time.
 * It also increases the finished_attempts count by one.
 * @param message The message to acknowledge.
 * @param client The database client. Must be part of the transaction where the message handling changes are done.
 * @param config The configuration settings that defines database schema.
 */
export const markMessageCompleted = async (
  { id }: StoredTransactionalMessage,
  client: DatabaseClient,
  { settings }: Pick<ListenerConfig, 'settings'>,
): Promise<void> => {
  await client.query(
    /* sql */ `UPDATE ${settings.dbSchema}.${settings.dbTable} SET processed_at = NOW(), finished_attempts = finished_attempts + 1 WHERE id = $1`,
    [id],
  );
};

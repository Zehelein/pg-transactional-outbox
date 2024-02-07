import { PoolClient } from 'pg';
import { ListenerConfig } from '../common/base-config';
import { StoredTransactionalMessage } from './transactional-message';

/**
 * Marks the message as abandoned by setting the abandoned_at date to the current date and time.
 * @param message The message to mark as abandoned.
 * @param client The database client. Must be part of the transaction where the error handling changes are done.
 * @param config The configuration settings that defines database schema.
 */
export const markMessageAbandoned = async (
  { id }: StoredTransactionalMessage,
  client: PoolClient,
  { settings }: Pick<ListenerConfig, 'settings'>,
): Promise<void> => {
  await client.query(
    /* sql */ `UPDATE ${settings.dbSchema}.${settings.dbTable} SET abandoned_at = NOW(), finished_attempts = finished_attempts + 1 WHERE id = $1;`,
    [id],
  );
};
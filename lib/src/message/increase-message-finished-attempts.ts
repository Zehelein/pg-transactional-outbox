import { PoolClient } from 'pg';
import { ListenerConfig } from '../common/base-config';
import { StoredTransactionalMessage } from './transactional-message';

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
    /* sql */ `
    UPDATE ${settings.dbSchema}.${settings.dbTable} SET finished_attempts = finished_attempts + 1 WHERE id = $1;`,
    [id],
  );
};

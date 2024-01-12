import { Pool, PoolClient } from 'pg';
import { ensureExtendedError } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { InboxMessage } from '../common/message';
import { InboxConfig } from '../inbox/inbox-listener';

/**
 * Create a DB client instance from the pool. This can be helpful if some message
 * handlers have to use different PostgreSQL roles or if you need to set some
 * PostgreSQL configuration values for the client connection.
 */
export interface MessageProcessingDbClientStrategy {
  /**
   * Decide based on the message which database client should be used to process
   * the message handler and the update to the message in the inbox table.
   * @param message The inbox message
   * @returns The PostgreSQL client to use to process the message
   */
  getClient: (message: InboxMessage) => Promise<PoolClient>;

  shutdown: () => Promise<void>;
}

/**
 * The default message processing uses a single pool based on the configured
 * `pgConfig` settings.
 */
export const defaultMessageProcessingDbClientStrategy = (
  config: InboxConfig,
  logger: TransactionalLogger,
): MessageProcessingDbClientStrategy => {
  const pool = new Pool(config.pgConfig);
  pool.on('error', (error) => {
    logger.error(
      ensureExtendedError(error, 'DB_ERROR'),
      'PostgreSQL pool error',
    );
  });
  return {
    getClient: async (_message: InboxMessage): Promise<PoolClient> =>
      await pool.connect(),
    shutdown: async () => {
      pool.removeAllListeners();
      try {
        await pool.end();
      } catch (error) {
        logger.error(
          ensureExtendedError(error, 'DB_ERROR'),
          'Message processing pool shutdown error',
        );
      }
    },
  };
};

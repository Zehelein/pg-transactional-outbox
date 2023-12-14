import { ClientBase, Pool } from 'pg';
import { TransactionalLogger } from '../common/logger';
import { InboxMessage } from '../common/message';
import { InboxConfig } from '../inbox/inbox-listener';

/**
 * Create a client instance from the pool. This can be helpful if some message
 * handlers have to use different PostgreSQL roles or if you need to set some
 * PostgreSQL configuration values for the client connection.
 */
export interface MessageProcessingClientStrategy {
  /**
   * Decide based on the message which database client should be used to process
   * the message handler and the update to the message in the inbox table.
   * @param message The inbox message
   * @returns The PostgreSQL client to use to process the message
   */
  getClient: (message: InboxMessage) => Promise<ClientBase>;

  shutdown: () => Promise<void>;
}

/**
 * The default message processing uses a single pool based on the configured
 * `pgConfig` settings.
 */
export const defaultMessageProcessingClientStrategy = (
  config: InboxConfig,
  logger: TransactionalLogger,
): MessageProcessingClientStrategy => {
  const pool = new Pool(config.pgConfig);
  pool.on('error', (err) => {
    logger.error(err, 'PostgreSQL pool error');
  });
  return {
    getClient: async (_message: InboxMessage): Promise<ClientBase> =>
      await pool.connect(),
    shutdown: async () => {
      pool.removeAllListeners();
      try {
        await pool.end();
      } catch (e) {
        logger.error(e, 'Message processing pool shutdown error');
      }
    },
  };
};

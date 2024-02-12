import { Pool } from 'pg';
import { ListenerConfig } from '../common/base-config';
import { DatabaseClient } from '../common/database';
import { ensureExtendedError } from '../common/error';
import { TransactionalLogger } from '../common/logger';

export interface DeleteOld {
  /** Delete messages where the processed field is older than the configured seconds */
  deleteProcessedInSec?: number;
  /** Delete messages where the abandoned field is older than the configured seconds */
  deleteAbandonedInSec?: number;
  /** Delete messages where the created field is older than the configured seconds */
  deleteAllInSec?: number;
}

/**
 * Deletes messages from the inbox. Please keep in mind that messages should
 * stay for some time especially in the inbox to handle message deduplication.
 * @param timesInSeconds Delete messages that
 * @param client A database client or directly the database pool. The used DB role must have permissions to delete messages. This is by default the case for the listener role.
 * @param config The configuration settings that defines the database schema.
 * @returns The number of deleted rows
 */
export const runMessageCleanupOnce = async (
  client: DatabaseClient,
  {
    settings: {
      dbSchema,
      dbTable,
      messageCleanupProcessedInSec,
      messageCleanupAbandonedInSec,
      messageCleanupAllInSec: messageCleanupAllInSec,
    },
  }: Pick<ListenerConfig, 'settings'>,
): Promise<number> => {
  if (
    !messageCleanupProcessedInSec &&
    !messageCleanupAbandonedInSec &&
    !messageCleanupAllInSec
  ) {
    return 0;
  }

  let sql = /* sql */ `DELETE FROM ${dbSchema}.${dbTable} WHERE false`;
  const insertValues = [];
  if (messageCleanupProcessedInSec) {
    insertValues.push(messageCleanupProcessedInSec);
    sql += /* sql */ ` OR processed_at < NOW() - ($${insertValues.length} || ' SECOND')::INTERVAL`;
  }
  if (messageCleanupAbandonedInSec) {
    insertValues.push(messageCleanupAbandonedInSec);
    sql += /* sql */ ` OR abandoned_at < NOW() - ($${insertValues.length} || ' SECOND')::INTERVAL`;
  }
  if (messageCleanupAllInSec) {
    insertValues.push(messageCleanupAllInSec);
    sql += /* sql */ ` OR created_at < NOW() - ($${insertValues.length} || ' SECOND')::INTERVAL`;
  }
  const rows = await client.query(`${sql} RETURNING id;`, insertValues);
  return rows.rowCount ?? 0;
};

/**
 *
 * @param pool The database pool that should be used to run the cleanup queries. The used DB role must have permissions to delete messages. This is by default the case for the listener role.
 * @param config The configuration settings that defines the database schema.
 * @param logger A logger object used to log processing issues.
 * @returns A timeout from setInterval that you must call once the cleanup should be stopped.
 */
export const runScheduledMessageCleanup = (
  pool: Pool,
  config: ListenerConfig,
  logger: TransactionalLogger,
): NodeJS.Timeout | undefined => {
  const { settings } = config;
  if (settings.messageCleanupIntervalInMs) {
    return setInterval(() => {
      runMessageCleanupOnce(pool, config)
        .then((deleted) => {
          if (deleted > 0) {
            logger.info(
              `Deleted ${deleted} ${config.outboxOrInbox} messages during cleanup.`,
            );
          } else {
            logger.trace(
              `Deleted no ${config.outboxOrInbox} messages during cleanup.`,
            );
          }
        })
        .catch((e) => {
          const err = ensureExtendedError(e, 'MESSAGE_CLEANUP_ERROR');
          logger.warn(err, 'Could not run the message cleanup logic.');
        });
    }, settings.messageCleanupIntervalInMs);
  }
  return undefined;
};

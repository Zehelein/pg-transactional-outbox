import { Pool, PoolClient, QueryConfig, QueryResult, QueryResultRow } from 'pg';
import { ListenerConfig } from '../common/base-config';
import { ensureExtendedError } from '../common/error';
import { TransactionalLogger } from '../common/logger';

export interface DeleteOld {
  /** Delete messages where the processed field is older than the configured seconds */
  deleteProcessed?: number;
  /** Delete messages where the abandoned field is older than the configured seconds */
  deleteAbandoned?: number;
  /** Delete messages where the created field is older than the configured seconds */
  deleteAll?: number;
}

interface Queryable {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  query<R extends QueryResultRow = any, I extends any[] = any[]>(
    queryTextOrConfig: string | QueryConfig<I>,
    values?: I,
  ): Promise<QueryResult<R>>;
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
  client: PoolClient | Queryable,
  {
    settings: {
      dbSchema,
      dbTable,
      messageCleanupProcessed,
      messageCleanupAbandoned,
      messageCleanupAll,
    },
  }: Pick<ListenerConfig, 'settings'>,
): Promise<number> => {
  if (
    !messageCleanupProcessed &&
    !messageCleanupAbandoned &&
    !messageCleanupAll
  ) {
    return 0;
  }

  let sql = /* sql */ `DELETE FROM ${dbSchema}.${dbTable} WHERE false`;
  const insertValues = [];
  if (messageCleanupProcessed) {
    insertValues.push(messageCleanupProcessed);
    sql += /* sql */ ` OR processed_at < NOW() - ($${insertValues.length} || ' SECOND')::INTERVAL`;
  }
  if (messageCleanupAbandoned) {
    insertValues.push(messageCleanupAbandoned);
    sql += /* sql */ ` OR abandoned_at < NOW() - ($${insertValues.length} || ' SECOND')::INTERVAL`;
  }
  if (messageCleanupAll) {
    insertValues.push(messageCleanupAll);
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
  if (settings.messageCleanupInterval) {
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
    }, settings.messageCleanupInterval * 1000);
  }
  return undefined;
};

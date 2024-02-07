import { TransactionalLogger } from '../common/logger';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { PollingListenerConfig } from './config';

// Basic type to support both the Pool directly as well as clients
interface QueryResult {
  rowCount: number | null;
  rows: any[];
}
interface Queryable {
  query(queryTextOrConfig: string, values?: any[]): Promise<QueryResult>;
}

/**
 * Gets the next inbox messages from the database and sets the locked_until
 * @param maxMessages The maximum number of messages to fetch.
 * @param client The database client to use for the query.
 * @param settings The settings object for the inbox table and function name.
 * @param logger The logger to use for logging.
 * @returns A promise that resolves to the query result object.
 */
export const getNextInboxMessages = async (
  maxMessages: number, // TODO: make the locked until time increment an input parameter?
  client: Queryable,
  settings: PollingListenerConfig,
  logger: TransactionalLogger,
): Promise<StoredTransactionalMessage[]> => {
  const messagesResult = await client.query(
    /* sql */ `SELECT * FROM ${
      settings.nextMessagesFunctionSchema ?? settings.dbSchema
    }.${settings.nextMessagesFunctionName}(${maxMessages});`,
  );
  logger.debug(
    messagesResult.rows.map((m) => m.id),
    `Found ${messagesResult.rowCount}`,
  );
  return messagesResult.rows.map(mapInbox);
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const mapInbox = (i: any): StoredTransactionalMessage => ({
  id: i.id,
  aggregateType: i.aggregate_type,
  aggregateId: i.aggregate_id,
  messageType: i.message_type,
  payload: i.payload,
  metadata: i.metadata as Record<string, unknown> | undefined,
  createdAt: i.created_at.toISOString(),
  concurrency: i.concurrency,
  finishedAttempts: i.finished_attempts,
  lockedUntil: i.locked_until?.toISOString(),
  startedAttempts: i.started_attempts,
  processedAt: i.processed_at?.toISOString(),
  abandonedAt: i.abandoned_at?.toISOString(),
  segment: i.segment,
});
import { DatabaseClient } from '../common/database';
import { OutboxOrInbox } from '../common/listener-config';
import { TransactionalLogger } from '../common/logger';
import { IsolationLevel, executeTransaction } from '../common/utils';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { FullPollingListenerSettings } from './config';

const lastLogTime = {
  inbox: 0,
  outbox: 0,
};

/**
 * Gets the next inbox messages from the database and sets the locked_until
 * @param maxMessages The maximum number of messages to fetch.
 * @param client The database client to use for the query.
 * @param settings The settings object for the inbox table and function name.
 * @param logger The logger to use for logging.
 * @param outboxOrInbox The outbox or inbox name
 * @returns A promise that resolves to the query result object.
 */
export const getNextMessagesBatch = async (
  maxMessages: number,
  client: DatabaseClient,
  settings: FullPollingListenerSettings,
  logger: TransactionalLogger,
  outboxOrInbox: OutboxOrInbox,
): Promise<StoredTransactionalMessage[]> => {
  const schema = settings.nextMessagesFunctionSchema;
  const func = settings.nextMessagesFunctionName;
  const lock = settings.nextMessagesLockInMs;

  const messagesResult = await executeTransaction(
    client,
    async (client) =>
      await client.query(
        /* sql */ `SELECT * FROM ${schema}.${func}(${maxMessages}, ${lock});`,
      ),
    IsolationLevel.RepeatableRead,
  );

  if (messagesResult.rowCount ?? 0 > 0) {
    logger.debug(
      messagesResult.rows.map((m) => m.id),
      `Found ${messagesResult.rowCount} ${outboxOrInbox} message(s) to process.`,
    );
    lastLogTime[outboxOrInbox] = Date.now();
  } else {
    if (lastLogTime[outboxOrInbox] <= Date.now() - 60_000) {
      logger.trace(
        `Found no unprocessed ${outboxOrInbox} messages in the last minute.`,
      );
      lastLogTime[outboxOrInbox] = Date.now();
    }
  }
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
  lockedUntil: i.locked_until?.toISOString() ?? null,
  startedAttempts: i.started_attempts,
  processedAt: i.processed_at?.toISOString() ?? null,
  abandonedAt: i.abandoned_at?.toISOString() ?? null,
  segment: i.segment,
});

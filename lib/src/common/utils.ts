import { Pool, PoolClient } from 'pg';
import { TransactionalOutboxInboxError, ensureExtendedError } from './error';
import { TransactionalLogger } from './logger';

/**
 * Sleep for a given amount of milliseconds
 * @param milliseconds The time in milliseconds to sleep
 * @returns The (void) promise to await
 */
export const sleep = async (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

/**
 * Run a promise but make sure to wait only a maximum amount of time for it to finish.
 * @param promise The promise to execute
 * @param timeoutMs The amount of time in milliseconds to wait for the promise to finish
 * @param failureMessage The message for the error if the timeout was reached
 * @returns The promise return value or a timeout error is thrown
 */
export const awaitWithTimeout = <T>(
  promise: () => Promise<T>,
  timeoutMs: number,
  failureMessage?: string,
): Promise<T> => {
  let timeoutHandle: NodeJS.Timeout;
  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    timeoutHandle = setTimeout(
      () =>
        reject(
          new TransactionalOutboxInboxError(
            failureMessage ?? 'Timeout',
            'TIMEOUT',
          ),
        ),
      timeoutMs,
    );
  });

  return Promise.race([promise(), timeoutPromise]).then((result) => {
    clearTimeout(timeoutHandle);
    return result;
  });
};

/**
 * PostgreSQL available isolation levels. The isolation level "Read uncommitted"
 * is the same as "Read committed" in PostgreSQL. And the readonly variants are
 * not usable as the message must be marked as processed or the
 * "finished_attempts" counter is updated for the message.
 */
export enum IsolationLevel {
  /** Highest protection - no serialization anomaly */
  Serializable = 'SERIALIZABLE',
  /** Second highest protection - no non-repeatable read */
  RepeatableRead = 'REPEATABLE READ',
  /** Lowest protection (same as read uncommitted in PG) - non-repeatable reads possible */
  ReadCommitted = 'READ COMMITTED',
}

/**
 * Open a transaction and execute the callback as part of the transaction.
 * @param client The PostgreSQL database client
 * @param callback The callback to execute DB commands with.
 * @param isolationLevel The database transaction isolation level. Falls back to the default PostgreSQL transaction level if not provided.
 * @returns The result of the callback (if any).
 * @throws Any error from the database or the callback.
 */
export const executeTransaction = async <T>(
  client: PoolClient,
  callback: (client: PoolClient) => Promise<T>,
  isolationLevel?: IsolationLevel,
): Promise<T> => {
  const isolation = Object.values(IsolationLevel).includes(
    isolationLevel as IsolationLevel,
  )
    ? isolationLevel
    : undefined;
  try {
    await client.query(
      isolation ? `START TRANSACTION ISOLATION LEVEL ${isolation}` : 'BEGIN',
    );
    const result = await callback(client);
    await client.query('COMMIT');
    client.release();
    return result;
  } catch (err) {
    const error = ensureExtendedError(err, 'DB_ERROR');
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      error.innerError = ensureExtendedError(rollbackError, 'DB_ERROR');
    }
    client.release(error);
    throw error;
  }
};

/**
 * Creates a PoolClient from the given pool.
 * @param pool A PostgreSQL database pool from which clients can be created.
 * @param logger A logger that will be registered for the on-error event of the client.
 * @returns The PoolClient object
 */
export const getClient = async (
  pool: Pool,
  logger?: TransactionalLogger,
): Promise<PoolClient> => {
  const client = await pool.connect();
  // The pool can return a new or an old client - we must register the event listener but should do so only once
  if (!client.listeners('error').length && logger) {
    client.on('error', (err) => {
      logger.error(
        ensureExtendedError(err, 'DB_ERROR'),
        'PostgreSQL client error',
      );
    });
  }
  return client;
};

/**
 * Process incoming promises in a pool with a maximum size. When that size is not
 * reached it tries to fill the processing pool up to the `getBatchSize`
 * number. It runs in an endless loop until the signal `stopped` variable is set
 * to true.
 * @param processingPool The promise items in the pool which are being worked on
 * @param getNextBatch Get the next items to fill up the pool. Gets the batch size input parameter from the `getBatchSize` parameter
 * @param getBatchSize Async function that gets the number of batch items that are currently processed
 * @param signal A signal object to stop the processing. Setting the stopped property to true will stop the loop
 * @param maxDelayMs The maximum number of milliseconds to wait before checking if new items are there
 */
export const processPool = async (
  processingPool: Set<Promise<void>>,
  getNextBatch: (batchSize: number) => Promise<Promise<void>[]>,
  getBatchSize: (currentlyProcessed: number) => Promise<number>,
  signal: { stopped: boolean },
  maxDelayMs: number,
): Promise<void> => {
  while (!signal.stopped) {
    // get the dynamic pool size - and one await is needed to allow event loop to continue
    const poolSize = await getBatchSize(processingPool.size);
    const diff = poolSize - processingPool.size;
    if (diff > 0) {
      const items = await getNextBatch(diff);
      items.forEach((item) => {
        processingPool.add(item);
        item.then(() => {
          processingPool.delete(item);
        });
      });
      const awaitItems = Array.from(processingPool);
      if (awaitItems.length < poolSize) {
        // If there are not enough items then try again at least after this duration
        awaitItems.push(sleep(maxDelayMs));
      }
      await Promise.race(awaitItems);
    }
  }
};

/**
 * Execute a function in a safe way e.g. pool.end where multiple callers could
 * have called it where the second one will throw an error.
 * @param it the function to execute
 */
export const justDoIt = async (
  it: (() => Promise<void>) | (() => void),
): Promise<void> => {
  try {
    await it();
  } catch {
    // noop
  }
};

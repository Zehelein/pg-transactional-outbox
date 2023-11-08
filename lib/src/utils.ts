import { Pool, PoolClient } from 'pg';
import { logger } from './logger';
import { ensureError } from './error';

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
      () => reject(new Error(failureMessage)),
      timeoutMs,
    );
  });

  return Promise.race([promise(), timeoutPromise]).then((result) => {
    clearTimeout(timeoutHandle);
    return result;
  });
};

/**
 * Open a transaction and execute the callback as part of the transaction.
 * @param pool The PostgreSQL database pool
 * @param callback The callback to execute DB commands with.
 * @returns The result of the callback (if any).
 * @throws Any error from the database or the callback.
 */
export const executeTransaction = async <T>(
  pool: Pool,
  callback: (client: PoolClient) => Promise<T>,
): Promise<T> => {
  let client: PoolClient | undefined = undefined;
  try {
    client = await getClient(pool);
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    client.release();
    return result;
  } catch (err) {
    const error = ensureError(err);
    try {
      await client?.query('ROLLBACK');
      client?.release(true);
    } catch (rollbackError) {
      // We report the initial error - this one is about DB connection issues
    }
    throw error;
  }
};

const getClient = async (pool: Pool) => {
  const client = await pool.connect();
  // The pool can return a new or an old client - we must register the event listener but should do so only once
  if (!client.listeners('error').length) {
    client.on('error', (err) => {
      logger().error(err, 'PostgreSQL client error');
    });
  }
  return client;
};

import { Pool, PoolClient } from 'pg';
import { logger } from './logger';

/**
 * Returns the error as verified Error object or wraps the input as Error.
 * @param error The error variable to check
 * @returns The error if the input was already an error otherwise a wrapped error.
 */
export const ensureError = (error: unknown): Error => {
  if (error instanceof Error) {
    return error;
  }
  return new Error(String(error));
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

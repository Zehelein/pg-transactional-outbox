import { Pool, PoolClient } from 'pg';
import { logger } from './logger';

export const ensureError = (error: unknown): Error => {
  if (error instanceof Error) {
    return error;
  }
  return new Error(String(error));
};

export const executeTransaction = async <T>(
  pool: Pool,
  callback: (client: PoolClient) => Promise<T>,
): Promise<T | Error> => {
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
    return error;
  }
};

const getClient = async (pool: Pool) => {
  const client = await pool.connect();
  // The pool can return a new or an old client - we must register the event listener but should do so only once
  if (!client.listeners('error').length) {
    client.on('error', (err) => {
      logger.error(err, 'PostgreSQL client error');
    });
  }
  return client;
};

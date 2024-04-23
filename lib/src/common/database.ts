import { Client, ClientBase, Pool, PoolClient } from 'pg';

/**
 * This interface combines the used query functions from the 'pg' library
 * `Pool`, `Client`, `ClientBase`, and `PoolClient`.
 */
export declare type DatabaseClient = Pool | PoolClient | ClientBase | Client;

/** Check if it is a pool client to correctly release it */
export const releaseIfPoolClient = (
  client: DatabaseClient,
  err?: Error | boolean,
): void => {
  if (client instanceof Client && 'release' in client) {
    (client as PoolClient).release(err);
  }
};

/** Check if the given error is based on a PostgreSQL serialization or deadlock error. */
export const isPgSerializationError = (error: unknown): boolean => {
  if (
    error &&
    typeof error === 'object' &&
    'code' in error &&
    // Trx Rollback: serialization failure = 40001 deadlock detected = 40P01
    (error.code === '40001' || error.code === '40P01')
  ) {
    return true;
  }
  return false;
};

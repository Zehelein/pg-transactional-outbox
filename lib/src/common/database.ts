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

import { ClientBase, Pool, PoolClient } from 'pg';
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
 * not usable as the inbox message must be marked as processed or the
 * "finished_attempts" counter is updated for the message.
 */
export enum IsolationLevel {
  Serializable = 'SERIALIZABLE',
  RepeatableRead = 'REPEATABLE READ',
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
  client: ClientBase | PoolClient,
  callback: (client: ClientBase) => Promise<T>,
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
    if (isPoolClient(client)) {
      client.release();
    }
    return result;
  } catch (err) {
    const error = ensureExtendedError(err, 'DB_ERROR');
    try {
      await client.query('ROLLBACK');
      if (isPoolClient(client)) {
        client.release(true);
      }
    } catch (rollbackError) {
      error.innerError = ensureExtendedError(rollbackError, 'DB_ERROR');
    }
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

function isPoolClient(object: ClientBase): object is PoolClient {
  return 'release' in object;
}

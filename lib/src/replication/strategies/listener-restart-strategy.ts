import { ClientConfig, Pool } from 'pg';
import {
  ExtendedError,
  MessageError,
  ensureExtendedError,
} from '../../common/error';
import { OutboxOrInbox } from '../../common/listener-config';
import { TransactionalLogger } from '../../common/logger';
import { FullReplicationListenerConfig } from '../config';

/**
 * When some error is caught in the outbox or inbox listener this strategy will
 * allow you to log/track the error and potentially remove the underlying issue
 * e.g. if the replication slot does not exist after a database failover. It
 * returns the wait time until the listener should try to connect again.
 */
export interface ReplicationListenerRestartStrategy {
  /**
   * Check based on the error how long the listener should wait to restart.
   * @param error The caught error object (non Error type errors are wrapped)
   * @param logger The logger instance to use for logging the error.
   * @param outboxOrInbox The outbox or inbox name
   * @returns The time in milliseconds how long the listener should wait before restarting
   */
  (
    error: ExtendedError,
    logger: TransactionalLogger,
    outboxOrInbox: OutboxOrInbox,
  ): Promise<number>;
}

/**
 * The default listener restart strategy checks if the error is a PostgreSQL
 * error. If the PostgreSQL error is about the replication slot being in use, it
 * logs a trace entry and waits for the configured `restartDelaySlotInUseInMs`
 * time. Otherwise, it logs an error entry and waits for the configured
 * `restartDelayInMs`.
 */
export const defaultReplicationListenerRestartStrategy = (
  config: FullReplicationListenerConfig,
): ReplicationListenerRestartStrategy => {
  return handleError(config);
};

/**
 * The default listener and slot restart strategy uses the same logic as the
 * `defaultListenerRestartStrategy`. In addition, it checks if a PostgreSQL error
 * is about the replication slot not existing (e.g. after a DB failover). Then
 * it tries to create the replication slot with the connection details of the
 * replication user slot and waits for the configured `restartDelayInMs`.
 */
export const defaultReplicationListenerAndSlotRestartStrategy = (
  config: FullReplicationListenerConfig,
): ReplicationListenerRestartStrategy => {
  return handleError(config, createReplicationSlot);
};

const handleError = (
  {
    settings: {
      restartDelayInMs,
      restartDelaySlotInUseInMs,
      dbReplicationSlot,
    },
    dbListenerConfig: pgReplicationConfig,
  }: FullReplicationListenerConfig,
  replicationSlotNotFoundCallback?: typeof createReplicationSlot,
): ReplicationListenerRestartStrategy => {
  return async (
    error: ExtendedError,
    logger: TransactionalLogger,
    outboxOrInbox: OutboxOrInbox,
  ): Promise<number> => {
    if ('routine' in error && error.routine === 'ReplicationSlotAcquire') {
      if ('code' in error && error.code === '55006') {
        logger.trace(
          error,
          `The replication slot for the ${outboxOrInbox} listener is currently in use.`,
        );
        return restartDelaySlotInUseInMs;
      } else if ('code' in error && error.code === '42704') {
        // replication slot not found - best effort to create it again
        logger.error(error, error.message);
        await replicationSlotNotFoundCallback?.(
          pgReplicationConfig,
          dbReplicationSlot,
          logger,
          outboxOrInbox,
        );
      }
      return restartDelayInMs;
    }

    if (
      !(error instanceof MessageError) &&
      error.constructor.name !== MessageError.name // needed for jest which has an "instanceof" bug
    ) {
      // Message based errors are already logged
      logger.error(error, `Transactional ${outboxOrInbox} listener error`);
    }
    return restartDelayInMs;
  };
};

const createReplicationSlot = async (
  pgReplicationConfig: ClientConfig,
  dbReplicationSlot: string,
  logger: TransactionalLogger,
  outboxOrInbox: string,
) => {
  const pool = new Pool(pgReplicationConfig);
  try {
    await pool.query(
      /** sql*/ `select pg_create_logical_replication_slot('${dbReplicationSlot}', 'pgoutput');`,
    );
  } catch (err) {
    logger.trace(
      ensureExtendedError(err, 'DB_ERROR'),
      `Failed to create the replication slot for the ${outboxOrInbox} which does not exist.`,
    );
  } finally {
    try {
      await pool.end();
    } catch {
      // ignore
    }
  }
};

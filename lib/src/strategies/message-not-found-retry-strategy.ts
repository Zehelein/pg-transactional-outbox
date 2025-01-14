import { FullListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';

/**
 * Decides if the initial message loading for locking should be retried in case
 * the message could not be found in the database. One scenario where this is
 * relevant is when the PostgreSQL database is under heavy load where the
 * logical replication signals that a message exists in the database but a
 * SELECT to lock the message does not find it (initially) in the DB.
 */
export interface MessageNotFoundRetryStrategy {
  /**
   * Checks if the message loading should be retried when the message was not
   * found.
   * @param message The message to find and lock
   * @param lockingAttempts The number of times a lock was attempted but the message was not found.
   * @returns retry: true if the message locking should be retried, otherwise false. And the delay on how long to wait for the next attempt.
   */
  (
    message: StoredTransactionalMessage,
    lockingAttempts: number,
  ): { retry: boolean; delayInMs: number };
}

/**
 * Get the default strategy for retrying to find and lock a message that was not
 * found. The default implementation will retry two more times.
 */
export const defaultMessageNotFoundRetryStrategy = (
  config: FullListenerConfig,
): MessageNotFoundRetryStrategy => {
  return (
    _message: StoredTransactionalMessage,
    lockingAttempts: number,
  ): { retry: boolean; delayInMs: number } => {
    return {
      retry: lockingAttempts <= config.settings.maxMessageNotFoundAttempts,
      delayInMs: config.settings.maxMessageNotFoundDelayInMs,
    };
  };
};

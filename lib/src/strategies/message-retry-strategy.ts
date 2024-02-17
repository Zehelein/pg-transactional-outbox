import { FullListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';

/**
 * Decides if a message should be attempted and what the maximum number of
 * attempts for that message should be.
 */
export interface MessageRetryStrategy {
  /**
   * Checks if the message should be retried after an error occurred. The number
   * of processing attempts (including the current) is available in the message
   * object.
   * @param message The stored message
   * @returns true if the message should be retried, otherwise false.
   */
  (message: StoredTransactionalMessage): boolean;
}

/**
 * Get the default message retry strategy. This strategy checks that the maximum
 * of finished attempts is not exceeded. The number can be defined in the
 * `config.settings.maxAttempts` variable.
 */
export const defaultMessageRetryStrategy = (
  config: FullListenerConfig,
): MessageRetryStrategy => {
  return (message: StoredTransactionalMessage): boolean =>
    message.finishedAttempts < config.settings.maxAttempts;
};

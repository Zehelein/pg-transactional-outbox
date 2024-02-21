import { ExtendedError } from '../common/error';
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
   * @param error The error that was thrown from the message handler or error handler
   * @param source If the error was thrown from the message handler, from the error handler, or from the code handling errors in the error handler.
   * @returns true if the message should be retried, otherwise false.
   */
  (
    message: StoredTransactionalMessage,
    error: ExtendedError,
    source: 'message-handler' | 'error-handler' | 'error-handler-error',
  ): boolean;
}

/**
 * Get the default message retry strategy. This strategy checks that the maximum
 * of finished attempts is not exceeded. The number can be defined in the
 * `config.settings.maxAttempts` variable. If the error is a PostgreSQL
 * serialization error it will always be retried. If another error was thrown
 * from the code handling errors in the error handler the message is not retired.
 */
export const defaultMessageRetryStrategy = (
  config: FullListenerConfig,
): MessageRetryStrategy => {
  return (
    message: StoredTransactionalMessage,
    error: ExtendedError,
    source: 'message-handler' | 'error-handler' | 'error-handler-error',
  ): boolean => {
    if (
      error.innerError &&
      'code' in error.innerError &&
      error.innerError.code === '40001'
    ) {
      // always retry PostgreSQL serialization_failure
      return true;
    }
    if (source === 'error-handler-error') {
      // If even the code that handles errors in the error handler throws an error the message is not retried.
      return false;
    }
    return message.finishedAttempts < config.settings.maxAttempts;
  };
};

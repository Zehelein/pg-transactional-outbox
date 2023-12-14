import { InboxMessage } from '../common/message';
import { InboxConfig } from '../inbox/inbox-listener';

/**
 * Decides if a message should be attempted and what the maximum number of
 * attempts for that message should be.
 */
export interface MessageRetryStrategy {
  /**
   * Checks if the message should be retried after an error occured. The number
   * of processing attempts (including the current) is available in the message
   * object.
   * @param message The inbox message
   * @returns true if the message should be retried, otherwise false.
   */
  (message: InboxMessage): boolean;
}

/**
 * Get the default message retry strategy. This strategy checks that the maximum
 * of finished attempts is not exceeded. The number can be defined in the
 * `config.settings.maxAttempts` variable and defaults to 5 attempts.
 */
export const defaultMessageRetryStrategy = (
  config: InboxConfig,
): MessageRetryStrategy => {
  const maxAttempts = config.settings.maxAttempts ?? 5;
  return (message: InboxMessage): boolean =>
    message.finishedAttempts < maxAttempts;
};
import { InboxMessage } from '../common/message';
import { InboxConfig } from '../inbox/inbox-listener';

/**
 * Decides if a message should be attempted and what the maximum number of
 * attempts for that message should be.
 */
export interface MessageRetryStrategy {
  /**
   * Checks if the message should be attempted. The current number of attempts
   * is available in the message object.
   * @param message The inbox message
   * @returns true if the message should be attempted/retried, otherwise false.
   */
  shouldAttempt: (message: InboxMessage) => boolean;
  /**
   * Calculates a number that defines how often this message should be attempted.
   * @param message The inbox message
   * @returns The maximum number of processing attempts.
   */
  maxAttempts: (message: InboxMessage) => number;
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
  return {
    shouldAttempt: (message: InboxMessage): boolean =>
      message.finishedAttempts < maxAttempts,
    maxAttempts: () => maxAttempts,
  };
};

import { OutboxMessage } from '../common/message';
import { OutboxConfig } from '../outbox/outbox-listener';

/**
 * Defines how much time a given message is allowed to take before the
 * processing of that message is cancelled.
 * @param message The outbox or inbox message
 * @returns The time in milliseconds for the timeout
 */
export interface MessageProcessingTimeoutStrategy {
  <T extends OutboxMessage>(message: T): number;
}

/**
 * Get the default message processing timeout strategy
 */
export const defaultMessageProcessingTimeoutStrategy =
  (config: OutboxConfig): MessageProcessingTimeoutStrategy =>
  () => {
    return config.settings.messageProcessingTimeout ?? 2_000;
  };

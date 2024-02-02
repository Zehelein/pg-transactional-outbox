import { ListenerConfig } from '../common/base-config';
import { TransactionalMessage } from '../message/message';

/**
 * Defines how much time in milliseconds a given message is allowed to take
 * before the processing of that message is cancelled.
 * @param message The outbox or inbox message
 * @returns The time in milliseconds for the timeout
 */
export interface MessageProcessingTimeoutStrategy {
  <T extends TransactionalMessage>(message: T): number;
}

/**
 * Get the default message processing timeout strategy which uses the
 * messageProcessingTimeout setting if this is defined or 2 seconds.
 */
export const defaultMessageProcessingTimeoutStrategy =
  (config: ListenerConfig): MessageProcessingTimeoutStrategy =>
  () => {
    return config.settings.messageProcessingTimeout ?? 2_000;
  };

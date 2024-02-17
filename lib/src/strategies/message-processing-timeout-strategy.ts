import { FullListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';

/**
 * Defines how much time in milliseconds a given message is allowed to take
 * before the processing of that message is cancelled.
 * @param message The outbox or inbox message
 * @returns The time in milliseconds for the timeout
 */
export interface MessageProcessingTimeoutStrategy {
  (message: StoredTransactionalMessage): number;
}

/**
 * Get the default message processing timeout strategy which uses the
 * messageProcessingTimeoutInMs setting.
 */
export const defaultMessageProcessingTimeoutStrategy =
  (config: FullListenerConfig): MessageProcessingTimeoutStrategy =>
  () => {
    return config.settings.messageProcessingTimeoutInMs;
  };

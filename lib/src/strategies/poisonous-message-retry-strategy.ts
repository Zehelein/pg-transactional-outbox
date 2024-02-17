import { FullListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';

/**
 * Decide based on the message, the poisonous attempts counter (which is
 * already increased by one), and the processing attempts if the message
 * should be retried again or not. This method is called if the
 * "started_attempts" and the "finished_attempts" differ by more than one.
 * @param message The potentially poisonous message
 * @returns true if it should be retried - otherwise false
 */
export interface PoisonousMessageRetryStrategy {
  (message: StoredTransactionalMessage): boolean;
}

/**
 * Get the default message retry strategy for poisonous messages. This strategy
 * checks that the difference between started attempts and finished attempts is
 * not exceeded. The number can be defined in the
 * `config.settings.maxPoisonousAttempts` variable.
 */
export const defaultPoisonousMessageRetryStrategy =
  (config: FullListenerConfig): PoisonousMessageRetryStrategy =>
  (message: StoredTransactionalMessage): boolean => {
    const diff = message.startedAttempts - message.finishedAttempts;
    return diff <= config.settings.maxPoisonousAttempts;
  };

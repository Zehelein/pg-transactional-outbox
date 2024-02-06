import { PollingConfig } from '../config';

/**
 * Define the scheduling strategy how often the database should be polled for
 * new messages.
 */
export interface PollingListenerSchedulingStrategy {
  /**
   * Returns the number of milliseconds to wait until the next attempt to find new messages to process.
   * @returns The time in milliseconds how long the listener should wait
   */
  (): Promise<number>;
}

/**
 * The default polling listener waits for the amount of time configured in the
 * `nextMessagesPollingInterval` settings field. Default is 500.
 * TODO: input parameters could be the elapsed time of the last message or the max/average recent message processing time
 */
export const defaultPollingListenerSchedulingStrategy = (
  config: PollingConfig,
): PollingListenerSchedulingStrategy => {
  return () =>
    Promise.resolve(config.settings.nextMessagesPollingInterval ?? 500);
};

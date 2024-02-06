import { PollingConfig } from '../config';

/**
 * Define the batch size strategy how many messages should be loaded at once.
 */
export interface PollingListenerBatchSizeStrategy {
  /**
   * Defines the number of records that should be loaded from the database table.
   * @returns The max number of records to load
   */
  (): Promise<number>;
}

/**
 * The default batch size strategy returns the configured value from the
 * `nextMessagesBatchSize`. Default is 5. But the first few times until the
 * batch size is reached it will tell to return only one message. This protects
 * against poisonous messages: if 5 messages would be taken during startup all
 * those 5 would be marked as poisonous if one of them fails.
 */
export const defaultPollingListenerBatchSizeStrategy = (
  config: PollingConfig,
): PollingListenerBatchSizeStrategy => {
  const max = config.settings.nextMessagesBatchSize ?? 5;
  let callsSinceStart = 1;
  return () => {
    let batchSize = max;
    if (callsSinceStart < max) {
      batchSize = 1;
      callsSinceStart++;
    }
    return Promise.resolve(batchSize);
  };
};

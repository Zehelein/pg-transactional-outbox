import { FullPollingListenerConfig } from '../config';

/**
 * Define the batch size strategy how many messages should be loaded at once.
 */
export interface PollingListenerBatchSizeStrategy {
  /**
   * Defines the number of records that should be loaded from the database table.
   * @param currentlyProcessed The number of currently processed messages
   * @returns The max number of records to load
   */
  (currentlyProcessed: number): Promise<number>;
}

/**
 * The default batch size strategy returns the configured value from the
 * `nextMessagesBatchSize`. But the first few times until the batch size is
 * reached it will tell to return only one message. This protects against
 * poisonous messages: if the full batch size would be taken during startup all
 * those messages would be marked as poisonous if one of them fails.
 */
export const defaultPollingListenerBatchSizeStrategy = (
  config: FullPollingListenerConfig,
): PollingListenerBatchSizeStrategy => {
  let callsSinceStart = 1;
  return () => {
    let batchSize = config.settings.nextMessagesBatchSize;
    if (callsSinceStart <= config.settings.nextMessagesBatchSize) {
      batchSize = 1;
      callsSinceStart++;
    }
    return Promise.resolve(batchSize);
  };
};

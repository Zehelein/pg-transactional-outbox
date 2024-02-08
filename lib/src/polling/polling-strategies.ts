import { HandlerStrategies } from '../handler/handler-strategies';
import { PollingListenerBatchSizeStrategy } from './strategies/batch-size-strategy';

export interface PollingMessageStrategies extends HandlerStrategies {
  /**
   * Define the batch size strategy how many messages should be loaded at once.
   */
  batchSizeStrategy: PollingListenerBatchSizeStrategy;
}

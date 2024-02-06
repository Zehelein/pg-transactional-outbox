import { HandlerStrategies } from '../handler/handler-strategies';
import { PollingListenerBatchSizeStrategy } from './strategies/batch-size-strategy';
import { PollingListenerSchedulingStrategy } from './strategies/polling-scheduling-strategy';

export interface PollingMessageStrategies extends HandlerStrategies {
  /**
   * Define the scheduling strategy how often the database should be polled for
   * new messages
   */
  schedulingStrategy: PollingListenerSchedulingStrategy;

  /**
   * Define the batch size strategy how many messages should be loaded at once.
   */
  batchSizeStrategy: PollingListenerBatchSizeStrategy;
}

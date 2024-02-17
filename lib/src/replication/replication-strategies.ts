import { HandlerStrategies } from '../handler/handler-strategies';
import { MessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';
import { ReplicationConcurrencyController } from './concurrency-controller/concurrency-controller';
import { ReplicationListenerRestartStrategy } from './strategies/listener-restart-strategy';

export interface ReplicationStrategies {
  /**
   * Define the concurrency strategy - defaults to using a mutex to guarantee
   * sequential message processing.
   */
  concurrencyStrategy: ReplicationConcurrencyController;

  /**
   * Defines the message processing timeout strategy. By default, it uses the
   * configured messageProcessingTimeoutInMs.
   */
  messageProcessingTimeoutStrategy: MessageProcessingTimeoutStrategy;

  /**
   * Returns the time in milliseconds after an error was caught until the
   * listener should try to restart itself. Offers an integration point to
   * act on the error and/or log it.
   */
  listenerRestartStrategy: ReplicationListenerRestartStrategy;
}

/** Optional strategies to provide custom logic for handling specific scenarios */
export interface ReplicationMessageStrategies
  extends ReplicationStrategies,
    HandlerStrategies {}

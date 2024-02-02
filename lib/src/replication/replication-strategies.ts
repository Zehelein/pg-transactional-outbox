import { ConcurrencyController } from '../concurrency-controller/concurrency-controller';
import { ListenerRestartStrategy } from '../strategies/listener-restart-strategy';
import { MessageProcessingDbClientStrategy } from '../strategies/message-processing-db-client-strategy';
import { MessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';
import { MessageProcessingTransactionLevelStrategy } from '../strategies/message-processing-transaction-level-strategy';
import { MessageRetryStrategy } from '../strategies/message-retry-strategy';
import { PoisonousMessageRetryStrategy } from '../strategies/poisonous-message-retry-strategy';

export interface ReplicationStrategies {
  /**
   * Define the concurrency strategy - defaults to using a mutex to guarantee
   * sequential message processing.
   */
  concurrencyStrategy: ConcurrencyController;

  /**
   * Defines the message processing timeout strategy. By default, it uses the
   * configured messageProcessingTimeout or falls back to a 15-second timeout.
   */
  messageProcessingTimeoutStrategy: MessageProcessingTimeoutStrategy;

  /**
   * Returns the time in milliseconds after an error was caught until the
   * listener should try to restart itself. Offers an integration point to
   * act on the error and/or log it.
   */
  listenerRestartStrategy: ListenerRestartStrategy;
}

/** Optional strategies to provide custom logic for handling specific scenarios */
export interface ReplicationMessageStrategies extends ReplicationStrategies {
  /**
   * Defines the PostgreSQL transaction level to use for handling a message.
   */
  messageProcessingTransactionLevelStrategy: MessageProcessingTransactionLevelStrategy;

  /**
   * Decides from what pool to take a database client. This can be helpful if some
   * message handlers have to run in higher trust roles than others.
   */
  messageProcessingDbClientStrategy: MessageProcessingDbClientStrategy;

  /**
   * Decides if a message should be retried or not based on the current amount
   * of attempts.
   */
  messageRetryStrategy: MessageRetryStrategy;

  /**
   * Decide based on the message, the poisonous attempts counter (which is
   * already increased by one), and the processing attempts if the message
   * should be retried again or not.
   */
  poisonousMessageRetryStrategy: PoisonousMessageRetryStrategy;
}

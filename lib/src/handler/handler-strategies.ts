import { MessageProcessingDbClientStrategy } from '../strategies/message-processing-db-client-strategy';
import { MessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';
import { MessageProcessingTransactionLevelStrategy } from '../strategies/message-processing-transaction-level-strategy';
import { MessageRetryStrategy } from '../strategies/message-retry-strategy';
import { PoisonousMessageRetryStrategy } from '../strategies/poisonous-message-retry-strategy';

export interface HandlerStrategies {
  /**
   * Defines the message processing timeout strategy. By default, it uses the
   * configured messageProcessingTimeout or falls back to a 15-second timeout.
   */
  messageProcessingTimeoutStrategy: MessageProcessingTimeoutStrategy;

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

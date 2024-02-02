import { IsolationLevel } from '../common/utils';
import { StoredTransactionalMessage } from '../message/message';

/**
 * Defines which transaction isolation level should be used to handle the given
 * message.
 * @param message The stored message that should be handled
 * @returns A transaction isolation level or undefined - which uses then the database default isolation level
 */
export interface MessageProcessingTransactionLevelStrategy {
  (message: StoredTransactionalMessage): IsolationLevel | undefined;
}

/**
 * Get the default message processing transaction level strategy
 */
export const defaultMessageProcessingTransactionLevelStrategy =
  (): MessageProcessingTransactionLevelStrategy => () => {
    return undefined;
  };

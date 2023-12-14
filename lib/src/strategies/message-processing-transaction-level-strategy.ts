import { InboxMessage } from '../common/message';
import { IsolationLevel } from '../common/utils';

/**
 * Defines which transaction isolation level should be used to handle the given
 * inbox message.
 * @param message The inbox message that should be handled
 * @returns A transaction isolation level or undefined - which uses then the database default isolation level
 */
export interface MessageProcessingTransactionLevelStrategy {
  (message: InboxMessage): IsolationLevel | undefined;
}

/**
 * Get the default message processing transaction level strategy
 */
export const defaultMessageProcessingTransactionLevelStrategy =
  (): MessageProcessingTransactionLevelStrategy => () => {
    return undefined;
  };
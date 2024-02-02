import { StoredTransactionalMessage } from '../message/message';
import { defaultMessageProcessingTransactionLevelStrategy } from './message-processing-transaction-level-strategy';

describe('defaultMessageProcessingTransactionLevelStrategy', () => {
  it('should return undefined', () => {
    const message = {
      id: '1',
      startedAttempts: 0,
      finishedAttempts: 0,
    } as StoredTransactionalMessage;
    const getTransactionLevel =
      defaultMessageProcessingTransactionLevelStrategy();

    expect(getTransactionLevel(message)).toBeUndefined();
  });
});

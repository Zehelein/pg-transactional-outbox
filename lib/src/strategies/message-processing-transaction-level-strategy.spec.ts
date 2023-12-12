import { InboxMessage } from '../common/message';
import { defaultMessageProcessingTransactionLevelStrategy } from './message-processing-transaction-level-strategy';

describe('defaultMessageProcessingTransactionLevelStrategy', () => {
  it('should return undefined', () => {
    const message = {
      id: '1',
      startedAttempts: 0,
      finishedAttempts: 0,
    } as InboxMessage;
    const getTransactionLevel =
      defaultMessageProcessingTransactionLevelStrategy();

    expect(getTransactionLevel(message)).toBeUndefined();
  });
});

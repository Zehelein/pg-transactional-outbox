import { FullListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { defaultMessageProcessingTimeoutStrategy } from './message-processing-timeout-strategy';

describe('defaultMessageProcessingTimeoutStrategy', () => {
  it('should return the configured value', () => {
    const getTimeout = defaultMessageProcessingTimeoutStrategy({
      settings: {
        messageProcessingTimeoutInMs: 123,
      },
    } as FullListenerConfig);

    expect(getTimeout({} as StoredTransactionalMessage)).toBe(123);
  });
});

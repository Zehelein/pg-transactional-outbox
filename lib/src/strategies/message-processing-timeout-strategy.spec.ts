import { ListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { defaultMessageProcessingTimeoutStrategy } from './message-processing-timeout-strategy';

describe('defaultMessageProcessingTimeoutStrategy', () => {
  it('should return the configured value', () => {
    const getTimeout = defaultMessageProcessingTimeoutStrategy({
      settings: {
        messageProcessingTimeoutInMs: 123,
      },
    } as ListenerConfig);

    expect(getTimeout({} as StoredTransactionalMessage)).toBe(123);
  });

  it('should return the default value when the config value is undefined', () => {
    const getTimeout = defaultMessageProcessingTimeoutStrategy({
      settings: {
        messageProcessingTimeoutInMs: undefined,
      },
    } as ListenerConfig);

    expect(getTimeout({} as StoredTransactionalMessage)).toBe(2000);
  });
});

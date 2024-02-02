import { ListenerConfig } from '../common/base-config';
import { TransactionalMessage } from '../message/message';
import { defaultMessageProcessingTimeoutStrategy } from './message-processing-timeout-strategy';

describe('defaultMessageProcessingTimeoutStrategy', () => {
  it('should return the configured value', () => {
    const getTimeout = defaultMessageProcessingTimeoutStrategy({
      settings: {
        messageProcessingTimeout: 123,
      },
    } as ListenerConfig);

    expect(getTimeout({} as TransactionalMessage)).toBe(123);
  });

  it('should return the default value when the config value is undefined', () => {
    const getTimeout = defaultMessageProcessingTimeoutStrategy({
      settings: {
        messageProcessingTimeout: undefined,
      },
    } as ListenerConfig);

    expect(getTimeout({} as TransactionalMessage)).toBe(2000);
  });
});

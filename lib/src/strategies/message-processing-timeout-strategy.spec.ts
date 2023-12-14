import { OutboxMessage } from '../common/message';
import { OutboxConfig } from '../outbox/outbox-listener';
import { defaultMessageProcessingTimeoutStrategy } from './message-processing-timeout-strategy';

describe('defaultMessageProcessingTimeoutStrategy', () => {
  it('should return the configured value', () => {
    const getTimeout = defaultMessageProcessingTimeoutStrategy({
      settings: {
        messageProcessingTimeout: 123,
      },
    } as OutboxConfig);

    expect(getTimeout({} as OutboxMessage)).toBe(123);
  });

  it('should return the default value when the config value is undefined', () => {
    const getTimeout = defaultMessageProcessingTimeoutStrategy({
      settings: {
        messageProcessingTimeout: undefined,
      },
    } as OutboxConfig);

    expect(getTimeout({} as OutboxMessage)).toBe(2000);
  });
});

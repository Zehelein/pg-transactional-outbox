import { InboxMessage } from '../common/message';
import { InboxConfig } from '../inbox/inbox-listener';
import { defaultMessageRetryStrategy } from './message-retry-strategy';

describe('defaultMessageRetryStrategy', () => {
  it('should use the default message retry strategy', () => {
    const config = {
      settings: { maxAttempts: undefined },
    } as InboxConfig;
    const message = {
      finishedAttempts: 0,
    } as InboxMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy.shouldAttempt(message)).toBe(true);
    expect(retryStrategy.maxAttempts(message)).toBe(5);
  });

  it('should use the configured message retry strategy', () => {
    const config: InboxConfig = {
      settings: {
        maxAttempts: 3,
      },
    } as InboxConfig;
    const message = {
      finishedAttempts: 2,
    } as InboxMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy.shouldAttempt(message)).toBe(true);
    expect(retryStrategy.maxAttempts(message)).toBe(3);
  });

  it('should use the default message retry strategy and not retry a message if the attempts are exceeded', () => {
    const config = {
      settings: { maxAttempts: undefined },
    } as InboxConfig;
    const message = {
      finishedAttempts: 5,
    } as InboxMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy.shouldAttempt(message)).toBe(false);
    expect(retryStrategy.maxAttempts(message)).toBe(5);
  });

  it('should use the configured message retry strategy and not retry a message if the attempts are exceeded', () => {
    const config: InboxConfig = {
      settings: {
        maxAttempts: 3,
      },
    } as InboxConfig;
    const message = {
      finishedAttempts: 3,
    } as InboxMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy.shouldAttempt(message)).toBe(false);
    expect(retryStrategy.maxAttempts(message)).toBe(3);
  });
});

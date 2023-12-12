import { InboxMessage } from '../common/message';
import { InboxConfig } from '../inbox/inbox-listener';
import { PoisonousCheck } from '../inbox/inbox-message-storage';
import { defaultPoisonousMessageRetryStrategy } from './poisonous-message-retry-strategy';

describe('defaultPoisonousMessageRetryStrategy', () => {
  it('should use the default message retry strategy settings', () => {
    const config = {
      settings: {},
    } as InboxConfig;
    const message = {
      finishedAttempts: 0,
    } as InboxMessage;
    const check: PoisonousCheck = {
      startedAttempts: 1,
      finishedAttempts: 0,
    };
    const retryStrategy = defaultPoisonousMessageRetryStrategy(config);

    expect(retryStrategy(message, check)).toBe(true);
  });

  it('should use the configured message retry strategy settings', () => {
    const config = {
      settings: {
        maxPoisonousAttempts: 2,
      },
    } as InboxConfig;
    const message = {
      finishedAttempts: 0,
    } as InboxMessage;
    const check: PoisonousCheck = {
      startedAttempts: 2,
      finishedAttempts: 0,
    };
    const retryStrategy = defaultPoisonousMessageRetryStrategy(config);

    expect(retryStrategy(message, check)).toBe(true);
  });

  it('should use the default message retry strategy settings and not retry for exceeded attempts', () => {
    const config = {
      settings: {},
    } as InboxConfig;
    const message = {
      finishedAttempts: 0,
    } as InboxMessage;
    const check: PoisonousCheck = {
      startedAttempts: 4,
      finishedAttempts: 0,
    };
    const retryStrategy = defaultPoisonousMessageRetryStrategy(config);

    expect(retryStrategy(message, check)).toBe(false);
  });

  it('should use the configured message retry strategy settings and not retry for exceeded attempts', () => {
    const config = {
      settings: {
        maxPoisonousAttempts: 2,
      },
    } as InboxConfig;
    const message = {
      finishedAttempts: 0,
    } as InboxMessage;
    const check: PoisonousCheck = {
      startedAttempts: 4,
      finishedAttempts: 1,
    };
    const retryStrategy = defaultPoisonousMessageRetryStrategy(config);

    expect(retryStrategy(message, check)).toBe(false);
  });
});

import { FullListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { defaultPoisonousMessageRetryStrategy } from './poisonous-message-retry-strategy';

describe('defaultPoisonousMessageRetryStrategy', () => {
  it('should use the configured message retry strategy settings', () => {
    const config = {
      settings: {
        maxPoisonousAttempts: 2,
      },
    } as FullListenerConfig;
    const message = {
      startedAttempts: 2,
      finishedAttempts: 0,
    } as StoredTransactionalMessage;
    const retryStrategy = defaultPoisonousMessageRetryStrategy(config);

    expect(retryStrategy(message)).toBe(true);
  });

  it('should use the default message retry strategy settings and not retry for exceeded attempts', () => {
    const config = {
      settings: {},
    } as FullListenerConfig;
    const message = {
      startedAttempts: 4,
      finishedAttempts: 0,
    } as StoredTransactionalMessage;

    const retryStrategy = defaultPoisonousMessageRetryStrategy(config);

    expect(retryStrategy(message)).toBe(false);
  });

  it('should use the configured message retry strategy settings and not retry for exceeded attempts', () => {
    const config = {
      settings: {
        maxPoisonousAttempts: 2,
      },
    } as FullListenerConfig;
    const message = {
      startedAttempts: 4,
      finishedAttempts: 1,
    } as StoredTransactionalMessage;
    const retryStrategy = defaultPoisonousMessageRetryStrategy(config);

    expect(retryStrategy(message)).toBe(false);
  });
});

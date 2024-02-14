import { ListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { defaultMessageRetryStrategy } from './message-retry-strategy';

describe('defaultMessageRetryStrategy', () => {
  it('should use the default message retry strategy', () => {
    const config = {
      settings: { maxAttempts: undefined },
    } as ListenerConfig;
    const message = {
      finishedAttempts: 0,
    } as StoredTransactionalMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy(message)).toBe(true);
  });

  it('should use the configured message retry strategy', () => {
    const config: ListenerConfig = {
      settings: {
        maxAttempts: 3,
      },
    } as ListenerConfig;
    const message = {
      finishedAttempts: 2,
    } as StoredTransactionalMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy(message)).toBe(true);
  });

  it('should use the default message retry strategy and not retry a message if the attempts are exceeded', () => {
    const config = {
      settings: { maxAttempts: undefined },
    } as ListenerConfig;
    const message = {
      finishedAttempts: 5,
    } as StoredTransactionalMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy(message)).toBe(false);
  });

  it('should use the configured message retry strategy and not retry a message if the attempts are exceeded', () => {
    const config: ListenerConfig = {
      settings: {
        maxAttempts: 3,
      },
    } as ListenerConfig;
    const message = {
      finishedAttempts: 3,
    } as StoredTransactionalMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy(message)).toBe(false);
  });
});

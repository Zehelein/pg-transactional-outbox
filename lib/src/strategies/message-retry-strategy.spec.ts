import { ensureExtendedError } from '../common/error';
import { FullListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { defaultMessageRetryStrategy } from './message-retry-strategy';

describe('defaultMessageRetryStrategy', () => {
  const defaultError = ensureExtendedError(new Error(), 'DB_ERROR');
  it('should use the configured message retry strategy', () => {
    const config = {
      settings: {
        maxAttempts: 3,
      },
    } as FullListenerConfig;
    const message = {
      finishedAttempts: 2,
    } as StoredTransactionalMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy(message, defaultError, 'message-handler')).toBe(true);
  });

  it('should use the default message retry strategy and not retry a message if the attempts are exceeded', () => {
    const config = {
      settings: { maxAttempts: undefined },
    } as FullListenerConfig;
    const message = {
      finishedAttempts: 5,
    } as StoredTransactionalMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy(message, defaultError, 'message-handler')).toBe(false);
  });

  it('should use the configured message retry strategy and not retry a message if the attempts are exceeded', () => {
    const config = {
      settings: {
        maxAttempts: 3,
      },
    } as FullListenerConfig;
    const message = {
      finishedAttempts: 3,
    } as StoredTransactionalMessage;
    const retryStrategy = defaultMessageRetryStrategy(config);

    expect(retryStrategy(message, defaultError, 'message-handler')).toBe(false);
  });

  it.each(['message-handler', 'error-handler', 'error-handler-error'])(
    'should retry serialization_failure errors even for exceeded attempts',
    (source) => {
      const config = {
        settings: {
          maxAttempts: 1,
        },
      } as FullListenerConfig;
      const message = {
        finishedAttempts: 1000,
      } as StoredTransactionalMessage;
      const serializationError = ensureExtendedError(new Error(), 'DB_ERROR');
      serializationError.innerError = new Error('Serialization Error');
      (serializationError.innerError as any).code = 40001;
      const retryStrategy = defaultMessageRetryStrategy(config);
      const src = source as
        | 'message-handler'
        | 'error-handler'
        | 'error-handler-error';

      expect(retryStrategy(message, serializationError, src)).toBe(false);
    },
  );
});

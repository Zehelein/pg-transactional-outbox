import { FullListenerConfig } from '../common/listener-config';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { defaultMessageNotFoundRetryStrategy } from './message-not-found-retry-strategy';

describe('defaultMessageNotFoundRetryStrategy', () => {
  it('should retry when the max retry count is exactly reached', () => {
    const config = {
      settings: {
        maxMessageNotFoundAttempts: 2,
        maxMessageNotFoundDelayInMs: 20,
      },
    } as FullListenerConfig;
    const message = {} as StoredTransactionalMessage;
    const retryStrategy = defaultMessageNotFoundRetryStrategy(config);

    expect(retryStrategy(message, 2)).toMatchObject({
      retry: true,
      delayInMs: 20,
    });
  });

  it('should not retry when the max retry count is exceeded', () => {
    const config = {
      settings: {
        maxMessageNotFoundAttempts: 2,
        maxMessageNotFoundDelayInMs: 30,
      },
    } as FullListenerConfig;
    const message = {} as StoredTransactionalMessage;
    const retryStrategy = defaultMessageNotFoundRetryStrategy(config);

    expect(retryStrategy(message, 3)).toMatchObject({
      retry: false,
      delayInMs: 30,
    });
  });
});

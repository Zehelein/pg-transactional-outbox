import { TransactionalOutboxInboxConfig } from '../replication/config';
import { defaultListenerRestartTimeStrategy } from './listener-restart-time-strategy';

describe('defaultListenerRestartTimeStrategy', () => {
  const config = {
    settings: {
      restartDelay: 250,
      restartDelaySlotInUse: 10_000,
    },
  } as TransactionalOutboxInboxConfig;

  it('should return restartDelaySlotInUse for PostgreSQL replication slot in use error', () => {
    const error = { code: '55006', routine: 'ReplicationSlotAcquire' };
    const strategy = defaultListenerRestartTimeStrategy(config);
    const result = strategy(error);
    expect(result).toBe(config.settings.restartDelaySlotInUse);
  });

  it('should return restartDelay for other errors', () => {
    const error = new Error('some_other_code');
    const strategy = defaultListenerRestartTimeStrategy(config);
    const result = strategy(error);
    expect(result).toBe(config.settings.restartDelay);
  });
});

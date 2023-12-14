import { TransactionalOutboxInboxConfig } from '../replication/config';

/**
 * When some error is caught in the inbox or outbox listener this strategy will
 * allow you to define the wait time until the listener should try to connect again.
 */
export interface ListenerRestartTimeStrategy {
  /**
   * Check based on the error how long it should wait for the listener restart.
   * @param error The caught error
   * @returns The time in milliseconds how long the listener should wait before restarting
   */
  (error: unknown): number;
}

/**
 * The default listener restart time strategy checks if the error message is a
 * PostgreSQL error telling that the replication slot is in use. If this is the
 * case it will wait for the configured `restartDelaySlotInUse` (default: 10sec)
 * and otherwise for the configured `restartDelay` (default: 250ms).
 */
export const defaultListenerRestartTimeStrategy = ({
  settings: { restartDelay, restartDelaySlotInUse },
}: TransactionalOutboxInboxConfig): ListenerRestartTimeStrategy => {
  return (error: unknown): number => {
    const err = error as Error & { code?: string; routine?: string };
    if (err?.code === '55006' && err?.routine === 'ReplicationSlotAcquire') {
      return restartDelaySlotInUse ?? 10_000;
    } else {
      return restartDelay ?? 250;
    }
  };
};

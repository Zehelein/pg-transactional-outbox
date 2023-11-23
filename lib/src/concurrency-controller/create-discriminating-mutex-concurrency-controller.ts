import { Mutex } from 'async-mutex';
import { OutboxMessage } from '../common/message';
import { ConcurrencyController } from './concurrency-controller';

const mutexMap = new Map<string, Mutex>();

/**
 * Use multiple mutex controllers - one per discriminator
 * @param discriminator The discriminator to find or create a mutex for
 * @returns The controller to acquire and release the mutex for a specific discriminator
 */
export const createDiscriminatingMutexConcurrencyController = (
  discriminator: (message: OutboxMessage) => string,
): ConcurrencyController => {
  return {
    /** Acquire a lock (if any) and return a function to release it. */
    acquire: (message: OutboxMessage): Promise<() => void> => {
      const d = discriminator(message);
      let mutex = mutexMap.get(d);
      if (mutex) {
        return mutex.acquire();
      }
      mutex = new Mutex();
      mutexMap.set(d, mutex);
      return mutex.acquire();
    },

    /** Cancel all pending locks. */
    cancel: () => {
      for (const mutex of mutexMap.values()) {
        mutex.cancel();
      }
      mutexMap.clear();
    },
  };
};

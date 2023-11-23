import { OutboxMessage } from '../common/message';
import { ConcurrencyController } from './concurrency-controller';
import { createDiscriminatingMutexConcurrencyController } from './create-discriminating-mutex-concurrency-controller';
import { createFullConcurrencyController } from './create-full-concurrency-controller';
import { createMutexConcurrencyController } from './create-mutex-concurrency-controller';

export type ConcurrencyStrategy =
  | 'mutex'
  | 'full-concurrency'
  | 'discriminating-mutex';

/**
 * Use different mutex controllers depending on the strategy. If a discriminating
 * mutex should be used the discriminator function must be supplied as well.
 * @param strategy Implements the logic which concurrency controller should be used e.g. based on the aggregate and message type
 * @param discriminator The discriminator to find or create a mutex for when using the discriminating mutex.
 * @returns The controller to acquire and release the mutex for a specific discriminator
 */
export const createStrategyConcurrencyController = (
  strategy: (message: OutboxMessage) => ConcurrencyStrategy,
  discriminator?: (message: OutboxMessage) => string,
): ConcurrencyController => {
  const fullConcurrencyController = createFullConcurrencyController();
  const mutexController = createMutexConcurrencyController();
  const discriminatingMutexController = discriminator
    ? createDiscriminatingMutexConcurrencyController(discriminator)
    : undefined;
  return {
    /** Acquire a lock (if any) and return a function to release it. */
    acquire: (message: OutboxMessage): Promise<() => void> => {
      switch (strategy(message)) {
        case 'full-concurrency':
          return fullConcurrencyController.acquire(message);
        case 'mutex':
          return mutexController.acquire(message);
        case 'discriminating-mutex':
          if (!discriminatingMutexController) {
            throw new Error(
              'A discriminating mutex controller was not configured.',
            );
          }
          return discriminatingMutexController.acquire(message);
      }
    },

    /** Cancel all controllers. */
    cancel: () => {
      fullConcurrencyController.cancel();
      mutexController.cancel();
      discriminatingMutexController?.cancel();
    },
  };
};

import { MessageError } from '../common/error';
import { OutboxMessage } from '../common/message';
import { ConcurrencyController } from './concurrency-controller';
import { createDiscriminatingMutexConcurrencyController } from './create-discriminating-mutex-concurrency-controller';
import { createFullConcurrencyController } from './create-full-concurrency-controller';
import { createMutexConcurrencyController } from './create-mutex-concurrency-controller';
import { createSemaphoreConcurrencyController } from './create-semaphore-concurrency-controller';

export type MultiConcurrencyType =
  | 'mutex'
  | 'semaphore'
  | 'full-concurrency'
  | 'discriminating-mutex';

/**
 * Use different mutex controllers depending on the desired concurrency level
 * for different messages. If a discriminating mutex should be used, the
 * discriminator function must be supplied as well. If a semaphore concurrency
 * controller should be used, the `maxSemaphoreParallelism` should be set (it
 * defaults to 5);
 * @param getConcurrencyType Implements the logic which concurrency controller should be used e.g. based on the aggregate and message type
 * @param discriminator The discriminator to find or create a mutex for when using the discriminating mutex.
 * @returns The controller to acquire and release the mutex for a specific discriminator
 */
export const createMultiConcurrencyController = (
  getConcurrencyType: (message: OutboxMessage) => MultiConcurrencyType,
  settings?: {
    discriminator?: (message: OutboxMessage) => string;
    maxSemaphoreParallelism?: number;
  },
): ConcurrencyController => {
  const fullConcurrencyController = createFullConcurrencyController();
  const mutexController = createMutexConcurrencyController();
  const discriminatingMutexController = settings?.discriminator
    ? createDiscriminatingMutexConcurrencyController(settings.discriminator)
    : undefined;
  const semaphore = createSemaphoreConcurrencyController(
    settings?.maxSemaphoreParallelism ?? 5,
  );
  return {
    /** Acquire a lock (if any) and return a function to release it. */
    acquire: (message: OutboxMessage): Promise<() => void> => {
      switch (getConcurrencyType(message)) {
        case 'full-concurrency':
          return fullConcurrencyController.acquire(message);
        case 'mutex':
          return mutexController.acquire(message);
        case 'semaphore':
          return semaphore.acquire(message);
        case 'discriminating-mutex':
          if (!discriminatingMutexController) {
            throw new MessageError(
              'A discriminating mutex controller was not configured.',
              'DISCRIMINATING_MUTEX_NOT_CONFIGURED',
              message,
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
      semaphore.cancel();
    },
  };
};

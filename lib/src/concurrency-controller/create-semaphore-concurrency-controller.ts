import { Semaphore } from 'async-mutex';
import { ConcurrencyController } from './concurrency-controller';

/**
 * Uses a semaphore to execute up to a given amount of messages in parallel. Any
 * additional message waits until a currently processed message finishes.
 * @returns The controller to acquire and release the semaphore and to cancel all semaphores
 */
export const createSemaphoreConcurrencyController = (
  maxParallel: number,
): ConcurrencyController => {
  const semaphore = new Semaphore(maxParallel);
  return {
    acquire: async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const [_, release] = await semaphore.acquire();
      return release;
    },
    cancel: semaphore.cancel.bind(semaphore),
  };
};

import { Mutex } from 'async-mutex';
import { ConcurrencyController } from './concurrency-controller';

/**
 * Uses a single mutex to execute a single message at a time in order
 * @returns The controller to acquire and release the mutex
 */
export const createMutexConcurrencyController = (): ConcurrencyController =>
  new Mutex();

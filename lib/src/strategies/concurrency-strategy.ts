import { ConcurrencyController } from '../concurrency-controller/concurrency-controller';
import { createMutexConcurrencyController } from '../concurrency-controller/create-mutex-concurrency-controller';

/**
 * The default concurrency strategy - which is the mutex concurrency controller
 * which guarantees sequential message processing.
 */
export const defaultConcurrencyStrategy = (): ConcurrencyController =>
  createMutexConcurrencyController();

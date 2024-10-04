import { Mutex } from 'async-mutex';
import { ReplicationConcurrencyController } from './concurrency-controller';

/**
 * Uses a single mutex to execute a single message at a time in the WAL sort order
 * @returns The controller to acquire and release the mutex
 */
export const createReplicationMutexConcurrencyController =
  (): ReplicationConcurrencyController => {
    const m = new Mutex();
    return {
      acquire: () => m.acquire(),
      cancel: () => m.cancel(),
    };
  };

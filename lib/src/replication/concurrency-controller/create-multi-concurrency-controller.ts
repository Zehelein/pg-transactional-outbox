import { TransactionalMessage } from '../../message/transactional-message';
import { ReplicationConcurrencyController } from './concurrency-controller';
import { createReplicationFullConcurrencyController } from './create-full-concurrency-controller';
import { createReplicationMutexConcurrencyController } from './create-mutex-concurrency-controller';
import { createReplicationSegmentMutexConcurrencyController } from './create-segment-mutex-concurrency-controller';
import { createReplicationSemaphoreConcurrencyController } from './create-semaphore-concurrency-controller';

export type ReplicationMultiConcurrencyType =
  | 'mutex'
  | 'semaphore'
  | 'full-concurrency'
  | 'segment-mutex';

/**
 * Use different mutex controllers depending on the desired concurrency level
 * for different messages. If a semaphore concurrency controller should be used,
 * the `maxSemaphoreParallelism` should be set (it defaults to 5);
 * @param getConcurrencyType Implements the logic which concurrency controller should be used e.g. based on the aggregate and message type
 * @returns The controller to acquire and release the mutex
 */
export const createReplicationMultiConcurrencyController = (
  getConcurrencyType: (
    message: TransactionalMessage,
  ) => ReplicationMultiConcurrencyType,
  settings?: {
    maxSemaphoreParallelism?: number;
  },
): ReplicationConcurrencyController => {
  const fullConcurrencyController =
    createReplicationFullConcurrencyController();
  const mutexController = createReplicationMutexConcurrencyController();
  const segmentMutexController =
    createReplicationSegmentMutexConcurrencyController();
  const semaphore = createReplicationSemaphoreConcurrencyController(
    settings?.maxSemaphoreParallelism ?? 5,
  );
  return {
    /** Acquire a lock (if any) and return a function to release it. */
    acquire: (message: TransactionalMessage): Promise<() => void> => {
      switch (getConcurrencyType(message)) {
        case 'full-concurrency':
          return fullConcurrencyController.acquire(message);
        case 'mutex':
          return mutexController.acquire(message);
        case 'semaphore':
          return semaphore.acquire(message);
        case 'segment-mutex':
          return segmentMutexController.acquire(message);
      }
    },

    /** Cancel all controllers. */
    cancel: () => {
      fullConcurrencyController.cancel();
      mutexController.cancel();
      segmentMutexController?.cancel();
      semaphore.cancel();
    },
  };
};

import { Mutex } from 'async-mutex';
import { TransactionalMessage } from '../../message/transactional-message';
import { ReplicationConcurrencyController } from './concurrency-controller';

const mutexMap = new Map<string | null | undefined, Mutex>();

/**
 * Use multiple mutex controllers - one for every unique segment of the messages
 * @returns The controller to acquire and release the mutex for a specific segment
 */
export const createReplicationSegmentMutexConcurrencyController =
  (): ReplicationConcurrencyController => {
    return {
      /** Acquire a lock (if any) and return a function to release it. */
      acquire: (message: TransactionalMessage): Promise<() => void> => {
        let mutex = mutexMap.get(message.segment);
        if (mutex) {
          return mutex.acquire();
        }
        mutex = new Mutex();
        mutexMap.set(message.segment, mutex);
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

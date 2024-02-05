import { ReplicationConcurrencyController } from '../concurrency-controller/concurrency-controller';
import { createReplicationMutexConcurrencyController } from '../concurrency-controller/create-mutex-concurrency-controller';

/**
 * The default concurrency strategy - which is the mutex concurrency controller
 * which guarantees sequential message processing.
 */
export const defaultReplicationConcurrencyStrategy =
  (): ReplicationConcurrencyController =>
    createReplicationMutexConcurrencyController();

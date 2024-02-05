import { TransactionalMessage } from '../../message/transactional-message';

/**
 * A concurrency controller that defines how concurrency must be handled when
 * processing messages: in parallel and/or sequentially or use some other logic.
 */
export interface ReplicationConcurrencyController {
  /** Acquire a lock (if any) and return a function to release it. */
  acquire(message: TransactionalMessage): Promise<() => void>;

  /** Cancel all pending locks. */
  cancel(): void;
}

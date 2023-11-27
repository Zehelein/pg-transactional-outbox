import { OutboxMessage } from '../common/message';

/**
 * A concurrency controller that defines how concurrency must be handled when
 * processing messages: in parallel and/or sequentially or use some other logic.
 */
export interface ConcurrencyController {
  /** Acquire a lock (if any) and return a function to release it. */
  acquire(message: OutboxMessage): Promise<() => void>;

  /** Cancel all pending locks. */
  cancel(): void;
}

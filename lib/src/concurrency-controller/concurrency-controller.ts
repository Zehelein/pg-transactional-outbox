import { OutboxMessage } from '../common/message';

export interface ConcurrencyController {
  /** Acquire a lock (if any) and return a function to release it. */
  acquire(message: OutboxMessage): Promise<() => void>;

  /** Cancel all pending locks. */
  cancel(): void;
}

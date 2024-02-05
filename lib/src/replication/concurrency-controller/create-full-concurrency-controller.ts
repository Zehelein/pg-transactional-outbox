import { ReplicationConcurrencyController } from './concurrency-controller';

/**
 * This controller allows full concurrency without any locking mechanism
 * @returns The controller with "empty" acquire and release functions
 */
export const createReplicationFullConcurrencyController =
  (): ReplicationConcurrencyController => ({
    // eslint-disable-next-line require-await
    async acquire() {
      return () => {
        // does not lock --> no need to release
      };
    },
    cancel(): void {
      // does not lock --> no way to cancel
    },
  });

import { logger } from './logger';

export const resilienceTest = (
  stopOutboxService: () => Promise<void>,
  startOutboxServiceIfStopped: () => Promise<void>,
): void => {
  setInterval(async () => {
    if (Math.random() > 0.9) {
      logger.warn(
        'Stop the outbox service to fake some WAL subscription outage (while still producing videos).',
      );
      await stopOutboxService();
    }
    if (Math.random() > 0.9) {
      logger.warn(
        'Restart the subscription if it was stopped. It will now catch up with the outbox messages from the WAL.',
      );
      await startOutboxServiceIfStopped();
    }
  }, 750);
};

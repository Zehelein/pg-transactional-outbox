import { LogicalReplicationService } from 'pg-logical-replication';

export const resilienceTest = (
  stopOutboxService: () => Promise<void>,
  startOutboxServiceIfStopped: () => void,
) => {
  const log = (logText: string) => console.log('\x1b[32m%s\x1b[0m', logText);

  setInterval(async () => {
    if (Math.random() > 0.9) {
      log(
        'Stop the outbox service to fake some WAL subscription outage (while still producing videos).',
      );
      await stopOutboxService();
    }
    if (Math.random() > 0.9) {
      log(
        'Restart the subscription if it was stopped. It will now catch up with the outbox messages from the WAL.',
      );
      startOutboxServiceIfStopped();
    }
  }, 750);
};

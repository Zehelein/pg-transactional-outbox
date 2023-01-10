import { ClientConfig } from 'pg';
import {
  LogicalReplicationService,
  PgoutputPlugin,
  Pgoutput,
} from 'pg-logical-replication';
import { OutboxMessage } from './outbox';
import { logger } from './logger';

/** The outbox service configuration */
export interface OutboxServiceConfig {
  /**
   * The "pg" library based settings to initialize the PostgreSQL connection for
   * the outbox service (with replication permissions)
   */
  pgReplicationConfig: ClientConfig;
  /** Outbox service specific configurations */
  settings: {
    /** The database schema name where the outbox table is located */
    outboxSchema: string;
    /** The name of the used PostgreSQL replication */
    postgresOutboxPub: string;
    /** The name of the used PostgreSQL logical replication slot */
    postgresOutboxSlot: string;
  };
}

const createService = (
  config: OutboxServiceConfig,
  callback: (message: OutboxMessage) => Promise<void>,
  errorListener: (err: Error) => Promise<void>,
) => {
  const service = new LogicalReplicationService(config.pgReplicationConfig, {
    acknowledge: { auto: false, timeoutSeconds: 0 },
  });
  service.on('data', async (lsn: string, log: Pgoutput.Message) => {
    if (
      log.tag === 'insert' &&
      log.relation.schema === config.settings.outboxSchema &&
      log.relation.name === 'outbox'
    ) {
      const msg = log.new;
      const om = {
        id: msg.id,
        aggregateType: msg.aggregate_type,
        aggregateId: msg.aggregate_id,
        eventType: msg.event_type,
        payload: msg.payload,
        createdAt: msg.created_at,
      };
      logger().trace(om, 'Received an outbox WAL message');
      try {
        await callback(om);
        await service.acknowledge(lsn);
      } catch (err) {
        // Do not acknowledge the outbox message in case of a message sending error
        logger().error({ ...om, err }, 'Could not send the message');
      }
    }
  });
  service.on('error', errorListener);
  return service;
};

/** Wait up to 10 seconds until the service started up  */
async function waitForServiceStart(flags: { started: boolean }) {
  const timeout = Date.now() + 10000; // 10 secs
  await new Promise((resolve, reject) => {
    (function waitForStarted() {
      if (flags.started) {
        return resolve(true);
      }
      if (Date.now() > timeout) {
        reject(
          new Error(
            'Timeout: the outbox service did not start in a reasonable time.',
          ),
        );
      }
      setTimeout(waitForStarted, 30);
    })();
  });
}

/**
 * Initialize the service to watch for outbox table inserts via logical replication.
 * @param config The configuration object with required values to connect to the WAL.
 * @param callback The callback is called to actually send the message through a message bus or other means.
 * @returns Functions for a clean shutdown and to help testing "outages" of the outbox service
 */
export const initializeOutboxService = async (
  config: OutboxServiceConfig,
  callback: (message: OutboxMessage) => Promise<void>,
): Promise<{
  stop: { (): Promise<void> };
  startIfStopped: { (): Promise<void> };
  shutdown: { (): Promise<void> };
}> => {
  const errorListener = async (err: Error) => {
    logger().error(err);
    // Stop the current instance and create a new instance e.g. if the DB connection failed
    await service.stop();
    service = createService(config, callback, errorListener);
  };

  let service = createService(config, callback, errorListener);
  // Waiting for the service to start - otherwise a shutdown can conflict with the ongoing initialization
  const flags = { started: false, wasShutDown: false };
  service.on('start', () => {
    flags.started = true;
  });
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [config.settings.postgresOutboxPub],
  });

  const subscribeToOutboxMessages = (): void => {
    service
      // `.subscribe` will start the replication and continue to listen until it is stopped
      .subscribe(plugin, config.settings.postgresOutboxSlot)
      // Log any error and restart the replication after a small timeout
      // The service will catch up with any events in the WAL once it restarts.
      .catch((e) => logger().error(e))
      .then(() => {
        if (!flags.wasShutDown) {
          setTimeout(subscribeToOutboxMessages, 300);
        }
      });
  };
  subscribeToOutboxMessages();
  await waitForServiceStart(flags);
  return {
    stop: async () => {
      await service.stop();
    },
    startIfStopped: async () => {
      if (service.isStop()) {
        subscribeToOutboxMessages();
      }
    },
    shutdown: async () => {
      flags.wasShutDown = true;
      service.removeAllListeners();
      service.stop().catch((e) => logger().error(e));
    },
  };
};

import { Config } from './config';
import {
  LogicalReplicationService,
  PgoutputPlugin,
  Pgoutput,
} from 'pg-logical-replication';
import { mapOutbox, OutboxMessage } from './outbox';

const createService = (
  config: Config,
  callback: (message: OutboxMessage) => Promise<void>,
  errorListener: (err: Error) => Promise<void>,
) => {
  const service = new LogicalReplicationService(
    {
      host: config.postgresHost,
      port: config.postgresPort,
      user: config.postgresOutboxRole,
      password: config.postgresOutboxRolePassword,
      database: config.postgresDatabase,
    },
    {
      acknowledge: { auto: false, timeoutSeconds: 0 },
    },
  );
  service.on('data', async (lsn: string, log: Pgoutput.Message) => {
    if (
      log.tag === 'insert' &&
      log.relation.schema === config.postgresOutboxSchema &&
      log.relation.name === 'outbox'
    ) {
      const om = mapOutbox(log.new);
      const identifier = `${om.aggregateType}.${om.eventType}.${om.aggregateId}`;
      console.log(`Received WAL message ${identifier}`);
      try {
        await callback(om);
        service.acknowledge(lsn);
      } catch (error) {
        // Do not acknowledge the outbox message in case of a message sending error
        console.error(`Could not send the message ${identifier}.`, error);
      }
    }
  });
  service.on('error', errorListener);
  return service;
};

/**
 * Initialize the service to watch for outbox table inserts.
 * @param config The configuration object with required values to connect to the WAL.
 * @param callback The callback is called to actually send the message through a message bus or other means.
 * @returns Functions to help testing "outages" of the outbox service
 */
export const initializeOutboxService = (
  config: Config,
  callback: (message: OutboxMessage) => Promise<void>,
) => {
  const errorListener = async (err: Error) => {
    console.error(err);
    // Stop the current instance and create a new instance e.g. if the DB connection failed
    await service.stop();
    service = createService(config, callback, errorListener);
  };

  let service = createService(config, callback, errorListener);
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [config.postgresOutboxPub],
  });

  const subscribeToOutboxMessages = (): void => {
    service
      // `.subscribe` will start the replication and continue to listen until it is stopped
      .subscribe(plugin, config.postgresOutboxSlot)
      // Log any error and restart the replication after a small timeout
      // The service will catch up with any events in the WAL once it restarts.
      .catch(console.error)
      .then(() => {
        setTimeout(subscribeToOutboxMessages, 100);
      });
  };
  subscribeToOutboxMessages();
  return {
    stop: async () => {
      await service.stop();
    },
    startIfStopped: () => {
      if (service.isStop()) {
        subscribeToOutboxMessages();
      }
    },
  };
};

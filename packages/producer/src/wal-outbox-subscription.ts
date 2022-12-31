import { Config } from './config';
import {
  LogicalReplicationService,
  PgoutputPlugin,
  Pgoutput,
} from 'pg-logical-replication';
import { mapOutbox, OutboxMessage } from './outbox';

/**
 * Provide this callback to actually send the messages through a message bus or other means.
 */
export interface OutboxMessageHandler {
  (message: OutboxMessage): Promise<void>;
}

/**
 * The outbox service with the replication service instance, PG output plugin, and the outbox replication slot name.
 */
export interface OutboxService {
  service: LogicalReplicationService;
  plugin: PgoutputPlugin;
  slotName: string;
}

/**
 * Initialize the service to watch for outbox table inserts.
 * @param config The configuration object with required values to connect to the WAL.
 * @param callback The callback to actually send the message through a message bus or other means.
 * @returns The outbox service instance.
 */
export const initializeOutboxService = (
  config: Config,
  callback: OutboxMessageHandler,
): OutboxService => {
  const service = new LogicalReplicationService({
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresOutboxRole,
    password: config.postgresOutboxRolePassword,
    database: config.postgresDatabase,
  });
  service.on('data', async (_lsn: string, log: Pgoutput.Message) => {
    if (
      log.tag === 'insert' &&
      log.relation.schema === config.postgresOutboxSchema &&
      log.relation.name === 'outbox'
    ) {
      const om = mapOutbox(log.new);
      console.log(
        `Sending message for ${om.aggregateType}.${om.eventType}.${om.aggregateId}`,
      );
      await callback(om);
    }
  });

  service.on('error', (err: Error) => {
    console.error(err);
  });

  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [config.postgresOutboxPub],
  });
  return { service, plugin, slotName: config.postgresOutboxSlot };
};

/**
 * Start listening to the replication slot for outbox message inserts and
 * restart the subscription if there is an error. The service will continue to
 * listen to the outbox messages until it is stopped.
 * @param outboxService The outbox service details
 */
export const subscribeToOutboxMessages = ({
  service,
  plugin,
  slotName,
}: OutboxService): void => {
  service
    // `.subscribe` will start the replication and continue to listen until it is stopped
    .subscribe(plugin, slotName)
    // Log any error and restart the replication after a small timeout
    // The service will catch up with any events in the WAL once it restarts.
    .catch((e) => {
      console.error(e);
    })
    .then(() => {
      setTimeout(subscribeToOutboxMessages, 100);
    });
};

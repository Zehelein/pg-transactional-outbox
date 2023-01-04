import { Config } from './config';
import {
  LogicalReplicationService,
  PgoutputPlugin,
  Pgoutput,
} from 'pg-logical-replication';
import { InboxMessage } from './inbox';
import { logger } from './logger';

export interface InboxMessageHandler {
  aggregateType: string;
  eventType: string;
  handle: (message: InboxMessage) => Promise<void>;
}

const createService = (
  config: Config,
  messageHandlers: InboxMessageHandler[],
  errorListener: (err: Error) => Promise<void>,
) => {
  const service = new LogicalReplicationService(
    {
      host: config.postgresHost,
      port: config.postgresPort,
      user: config.postgresInboxRole,
      password: config.postgresInboxRolePassword,
      database: config.postgresDatabase,
    },
    {
      acknowledge: { auto: false, timeoutSeconds: 0 },
    },
  );
  service.on('data', async (lsn: string, log: Pgoutput.Message) => {
    if (
      log.tag === 'insert' &&
      log.relation.schema === config.postgresInboxSchema &&
      log.relation.name === 'inbox'
    ) {
      const msg = log.new;
      const im = {
        id: msg.id,
        aggregateType: msg.aggregate_type,
        aggregateId: msg.aggregate_id,
        eventType: msg.event_type,
        payload: msg.payload,
        createdAt: msg.created_at,
        retries: msg.retries,
      };
      logger.trace(log, 'Received WAL inbox message');
      try {
        await Promise.all(
          messageHandlers
            .filter(
              ({ aggregateType, eventType }) =>
                aggregateType === im.aggregateType &&
                eventType === im.eventType,
            )
            .map(({ handle }) => handle(im)),
        );
        service.acknowledge(lsn);
      } catch (error) {
        // Do not acknowledge the inbox message in case of a message sending error
        logger.error({ ...msg, error }, 'Could not process the inbox message');
      }
    }
  });
  service.on('error', errorListener);
  return service;
};

/**
 * Initialize the service to watch for inbox table inserts.
 * @param config The configuration object with required values to connect to the WAL.
 * @param callback The callback is called to actually process the received message.
 * @returns Functions to help testing "outages" of the inbox service
 */
export const initializeInboxService = (
  config: Config,
  messageHandlers: InboxMessageHandler[],
): {
  stop: { (): Promise<void> };
  startIfStopped: { (): void };
} => {
  const errorListener = async (err: Error) => {
    logger.error(err);
    // Stop the current instance and create a new instance e.g. if the DB connection failed
    await service.stop();
    service = createService(config, messageHandlers, errorListener);
  };

  let service = createService(config, messageHandlers, errorListener);
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [config.postgresInboxPub],
  });

  const subscribeToInboxMessages = (): void => {
    service
      // `.subscribe` will start the replication and continue to listen until it is stopped
      .subscribe(plugin, config.postgresInboxSlot)
      // Log any error and restart the replication after a small timeout
      // The service will catch up with any events in the WAL once it restarts.
      .catch(logger.error)
      .then(() => {
        setTimeout(subscribeToInboxMessages, 100);
      });
  };
  subscribeToInboxMessages();
  return {
    stop: async () => {
      await service.stop();
    },
    startIfStopped: () => {
      if (service.isStop()) {
        subscribeToInboxMessages();
      }
    },
  };
};

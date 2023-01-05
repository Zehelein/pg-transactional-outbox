import { Config } from './config';
import {
  LogicalReplicationService,
  PgoutputPlugin,
  Pgoutput,
} from 'pg-logical-replication';
import {
  InboxMessage,
  InboxError,
  verifyInbox,
  ackInbox,
  nackInbox,
} from './inbox';
import { logger } from './logger';
import { ClientBase, Pool } from 'pg';
import { ensureError, executeTransaction } from './utils';

const createPgPool = (config: Config) => {
  const pool = new Pool({
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresLoginRole,
    password: config.postgresLoginRolePassword,
    database: config.postgresDatabase,
  });
  pool.on('error', (err) => {
    logger.error(err, 'PostgreSQL pool error');
  });
  return pool;
};

const initializeReplicationService = (config: Config) => {
  return new LogicalReplicationService(
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
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const mapInboxMessage = (msg: Record<string, any>): InboxMessage => {
  return {
    id: msg.id,
    aggregateType: msg.aggregate_type,
    aggregateId: msg.aggregate_id,
    eventType: msg.event_type,
    payload: msg.payload,
    createdAt: msg.created_at,
    retries: msg.retries,
  };
};

/**
 * Execute the inbox verification, the actual message handler, and marking the
 * inbox message as processed in one transaction.
 */
const handleMessage = async (
  message: InboxMessage,
  messageHandlers: InboxMessageHandler[],
  pool: Pool,
  config: Config,
) => {
  await executeTransaction(pool, async (client) => {
    await verifyInbox(message, client, config);
    await Promise.all(
      messageHandlers
        .filter(
          ({ aggregateType, eventType }) =>
            aggregateType === message.aggregateType &&
            eventType === message.eventType,
        )
        .map(({ handle }) => handle(message, client)),
    );
    await ackInbox(message, client, config);
  });
};

/**
 * Handle specific error cases (message already processed/not found) by
 * acknowledging the inbox WAL message. For other errors increase the retry
 * counter of the message and retry it later.
 */
const resolveMessageHandlingError = async (
  error: Error,
  message: InboxMessage,
  lsn: string,
  service: LogicalReplicationService,
  pool: Pool,
  config: Config,
) => {
  try {
    if (
      error instanceof InboxError &&
      (error.code === 'ALREADY_PROCESSED' ||
        error.code === 'INBOX_MESSAGE_NOT_FOUND')
    ) {
      service.acknowledge(lsn);
      logger.error({ ...message, err: error }, error.message);
    } else {
      await executeTransaction(pool, async (client) => {
        logger.error({ ...message, err: error }, 'Message handling failed.');
        const action = await nackInbox(message, client, config);
        if (action === 'RETRIES_EXCEEDED') {
          service.acknowledge(lsn);
        }
      });
    }
  } catch (error) {
    logger.error(
      { ...message, err: error },
      'The message handling error handling failed.',
    );
  }
};

const createService = (
  config: Config,
  messageHandlers: InboxMessageHandler[],
  errorListener: (err: Error) => Promise<void>,
) => {
  const pool = createPgPool(config);
  const service = initializeReplicationService(config);
  service.on('data', async (lsn: string, log: Pgoutput.Message) => {
    if (
      log.tag === 'insert' &&
      log.relation.schema === config.postgresInboxSchema &&
      log.relation.name === 'inbox'
    ) {
      const message: InboxMessage = mapInboxMessage(log.new);
      logger.trace(message, 'Received WAL inbox message');
      try {
        await handleMessage(message, messageHandlers, pool, config);
        // There is a tiny delay/chance that the above succeeds and this fails. But the message
        // in the DB is marked as processed. This is checked in the "verifyInbox" call.
        service.acknowledge(lsn);
      } catch (error) {
        // Do not acknowledge the inbox message in case of a message handling error
        await resolveMessageHandlingError(
          ensureError(error),
          message,
          lsn,
          service,
          pool,
          config,
        );
      }
    }
  });
  service.on('error', errorListener);
  return service;
};

/**
 * Message handler for a specific aggregate type and event type.
 */
export interface InboxMessageHandler {
  aggregateType: string;
  eventType: string;
  /**
   * Custom business logic to handle the inbox message.
   * @param message The inbox message with the payload to handle
   * @param client The database client that is part of a transaction to safely handle the inbox message
   * @throws If something failed and the inbox message should NOT be acknowledged - throw an error.
   */
  handle: (message: InboxMessage, client: ClientBase) => Promise<void>;
}

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
      .catch(logger.error.bind(logger))
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

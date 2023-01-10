import { ClientBase, ClientConfig, Pool } from 'pg';
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
import { ensureError, executeTransaction } from './utils';

/** The inbox service configuration */
export interface InboxServiceConfig {
  /**
   * Database connection details. The user needs update permission to the inbox.
   */
  pgConfig: ClientConfig;
  /**
   * Database connection details for the replication role. The user needs
   * replication permissions to read from the write ahead log.
   */
  pgReplicationConfig: ClientConfig;
  /** Inbox service specific configurations */
  settings: {
    /** The database schema name of the inbox table */
    inboxSchema: string;
    /** The name of the used PostgreSQL replication */
    postgresInboxPub: string;
    /** The name of the used PostgreSQL logical replication slot */
    postgresInboxSlot: string;
  };
}

const createPgPool = (config: InboxServiceConfig) => {
  const pool = new Pool(config.pgConfig);
  pool.on('error', (err) => {
    logger().error(err, 'PostgreSQL pool error');
  });
  return pool;
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
 * Executes the inbox verification, the actual message handler, and marks the
 * inbox message as processed in one transaction.
 */
const handleMessage = async (
  message: InboxMessage,
  messageHandlers: InboxMessageHandler[],
  pool: Pool,
  config: InboxServiceConfig,
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
 * acknowledging the inbox WAL message. For other errors: increase the retry
 * counter of the message and retry it later.
 */
const resolveMessageHandlingError = async (
  error: Error,
  message: InboxMessage,
  lsn: string,
  service: LogicalReplicationService,
  pool: Pool,
  config: InboxServiceConfig,
) => {
  try {
    if (
      error instanceof InboxError &&
      (error.code === 'ALREADY_PROCESSED' ||
        error.code === 'INBOX_MESSAGE_NOT_FOUND')
    ) {
      await service.acknowledge(lsn);
      logger().error({ ...message, err: error }, error.message);
    } else {
      await executeTransaction(pool, async (client) => {
        logger().error({ ...message, err: error }, 'Message handling failed.');
        const action = await nackInbox(message, client, config);
        if (action === 'RETRIES_EXCEEDED') {
          await service.acknowledge(lsn);
        }
      });
    }
  } catch (error) {
    logger().error(
      { ...message, err: error },
      'The message handling error handling failed.',
    );
  }
};

const createService = (
  pool: Pool,
  config: InboxServiceConfig,
  messageHandlers: InboxMessageHandler[],
  errorListener: (err: Error) => Promise<void>,
) => {
  const service = new LogicalReplicationService(config.pgReplicationConfig, {
    acknowledge: { auto: false, timeoutSeconds: 0 },
  });
  service.on('data', async (lsn: string, log: Pgoutput.Message) => {
    if (
      log.tag === 'insert' &&
      log.relation.schema === config.settings.inboxSchema &&
      log.relation.name === 'inbox'
    ) {
      const message: InboxMessage = mapInboxMessage(log.new);
      logger().trace(message, 'Received WAL inbox message');
      try {
        // There is a small chance that the message handling including marking
        // the message in the inbox table as processed, but the WAL message
        // acknowledging fails. The "verifyInbox" guards against this issue.
        await handleMessage(message, messageHandlers, pool, config);
        await service.acknowledge(lsn);
      } catch (error) {
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
            'Timeout: the inbox service did not start in a reasonable time.',
          ),
        );
      }
      setTimeout(waitForStarted, 30);
    })();
  });
}

/**
 * Message handler for a specific aggregate type and event type.
 */
export interface InboxMessageHandler {
  /** The aggregate root type */
  aggregateType: string;
  /** The name of the event created for the aggregate type. */
  eventType: string;
  /**
   * Custom business logic to handle a message that was stored in the inbox.
   * @param message The inbox message with the payload to handle.
   * @param client The database client that is part of a transaction to safely handle the inbox message.
   * @throws If something failed and the inbox message should NOT be acknowledged - throw an error.
   */
  handle: (message: InboxMessage, client: ClientBase) => Promise<void>;
}

/**
 * Initialize the service to watch for inbox table inserts.
 * @param config The configuration object with required values to connect to the WAL.
 * @param messageHandlers A list of message handlers to handle the inbox messages. I
 * @returns Functions for a clean shutdown and to help testing "outages" of the inbox service
 */
export const initializeInboxService = async (
  config: InboxServiceConfig,
  messageHandlers: InboxMessageHandler[],
): Promise<{
  shutdown: { (): Promise<void> };
}> => {
  const errorListener = async (err: Error) => {
    logger().error(err);
    // Stop the current instance and create a new instance e.g. if the DB connection failed
    await service.stop();
    service = createService(pool, config, messageHandlers, errorListener);
  };

  const pool = createPgPool(config);
  let service = createService(pool, config, messageHandlers, errorListener);
  // Waiting for the service to start - otherwise a shutdown can conflict with the ongoing initialization
  const flags = { started: false, wasShutDown: false };
  service.on('start', () => {
    flags.started = true;
  });
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [config.settings.postgresInboxPub],
  });

  const subscribeToInboxMessages = (): void => {
    service
      // `.subscribe` will start the replication and continue to listen until it is stopped
      .subscribe(plugin, config.settings.postgresInboxSlot)
      // Log any error and restart the replication after a small timeout
      // The service will catch up with any events in the WAL once it restarts.
      .catch((e) => logger().error(e))
      .then(() => {
        if (!flags.wasShutDown) {
          setTimeout(subscribeToInboxMessages, 300);
        }
      });
  };
  subscribeToInboxMessages();
  await waitForServiceStart(flags);
  return {
    shutdown: async () => {
      flags.wasShutDown = true;
      pool.removeAllListeners();
      pool.end().catch((e) => logger().error(e));
      service.removeAllListeners();
      service.stop().catch((e) => logger().error(e));
    },
  };
};

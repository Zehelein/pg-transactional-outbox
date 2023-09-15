import { ClientBase, ClientConfig, Pool } from 'pg';
import { verifyInbox, ackInbox, nackInbox } from './inbox';
import { createService, ServiceConfig } from './local-replication-service';
import { logger } from './logger';
import { InboxMessage } from './models';
import { executeTransaction } from './utils';

/** The inbox service configuration */
export type InboxServiceConfig = ServiceConfig & {
  /**
   * Database connection details. The user needs update permission to the inbox.
   */
  pgConfig: ClientConfig;

  settings: ServiceConfig['settings'] & {
    /**
     * The maximum number of retries to handle an incoming inbox message. Defaults to 5.
     */
    maxRetries?: number;
  };
};

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
): Promise<[shutdown: { (): Promise<void> }]> => {
  const pool = createPgPool(config);
  const messageHandler = createMessageHandler(messageHandlers, pool, config);
  const errorResolver = createErrorResolver(pool, config);
  const [shutdown] = createService(
    config,
    messageHandler,
    errorResolver,
    mapInboxRetries,
  );
  return [
    async () => {
      pool.removeAllListeners();
      pool
        .end()
        .catch((e) => logger().error(e, 'PostgreSQL pool shutdown error'));
      shutdown().catch((e) =>
        logger().error(e, 'Inbox service shutdown error'),
      );
    },
  ];
};

const createPgPool = (config: InboxServiceConfig) => {
  const pool = new Pool(config.pgConfig);
  pool.on('error', (err) => {
    logger().error(err, 'PostgreSQL pool error');
  });
  return pool;
};

/**
 * Executes the inbox verification, the actual message handler, and marks the
 * inbox message as processed in one transaction.
 */
const createMessageHandler = (
  messageHandlers: InboxMessageHandler[],
  pool: Pool,
  config: InboxServiceConfig,
) => {
  return async (message: InboxMessage) => {
    await executeTransaction(pool, async (client) => {
      const result = await verifyInbox(message, client, config);
      if (result === true) {
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
      } else {
        logger().warn(
          message,
          `Received inbox message cannot be processed: ${result}`,
        );
      }
    });
  };
};

/**
 * Increase the retry counter of the message and retry it later.
 */
const createErrorResolver = (pool: Pool, config: InboxServiceConfig) => {
  /**
   * An error handler that will increase the inbox retries count on transient errors.
   * @param message: the InboxMessage that failed to be processed
   * @param error: the error that was thrown while processing the message
   */
  return async (message: InboxMessage, _error: Error): Promise<void> => {
    try {
      await executeTransaction(pool, async (client) => {
        // This could be extended to check for transient vs. persistent errors
        // to acknowledge persistent errors to not further retry the message.
        await nackInbox(message, client, config);
      });
    } catch (error) {
      logger().error(
        { ...message, err: error },
        'The error handling of the message failed.',
      );
    }
  };
};

/** The local replication service maps by default only the outbox properties */
const mapInboxRetries = (input: object) => {
  if (
    'retries' in input &&
    typeof input.retries === 'number' &&
    'processed_at' in input &&
    (input.processed_at == null || input.processed_at instanceof Date)
  ) {
    return {
      retries: input.retries,
      processedAt: input.processed_at?.toISOString(),
    };
  }
  return {};
};

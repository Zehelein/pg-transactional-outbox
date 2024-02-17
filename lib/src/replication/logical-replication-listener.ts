import { EventEmitter } from 'events';
import { Client, ClientConfig, Connection, Pool } from 'pg';
import { Pgoutput, PgoutputPlugin } from 'pg-logical-replication';
import { AbstractPlugin } from 'pg-logical-replication/dist/output-plugins/abstract.plugin';
import {
  ExtendedError,
  MessageError,
  TransactionalOutboxInboxError,
  ensureExtendedError,
} from '../common/error';
import { OutboxOrInbox } from '../common/listener-config';
import { TransactionalLogger } from '../common/logger';
import { awaitWithTimeout } from '../common/utils';
import { runScheduledMessageCleanup } from '../message/message-cleanup';
import {
  StoredTransactionalMessage,
  TransactionalMessage,
} from '../message/transactional-message';
import { MessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';
import { createAcknowledgeManager } from './acknowledge-manager';
import { ReplicationConcurrencyController } from './concurrency-controller/concurrency-controller';
import {
  FullReplicationListenerConfig,
  FullReplicationListenerSettings,
} from './config';
import { ReplicationStrategies } from './replication-strategies';

/** connection exists on the Client but is not typed */
type ReplicationClient = Client & {
  connection: Connection & { sendCopyFromChunk: (buffer: Buffer) => void };
};

/**
 * Initiate the outbox or inbox listener to listen for WAL messages.
 * @param config The replication connection settings and general replication settings
 * @param messageHandler The message handler that handles the outbox/inbox message
 * @param errorHandler A handler that can decide if the WAL message should be acknowledged (permanent_error) or not (transient_error which restarts the logical replication listener)
 * @param logger A logger instance for logging trace up to error logs
 * @param strategies Strategies to provide custom logic for handling specific scenarios
 * @returns A function to stop the logical replication listener
 */
export const createLogicalReplicationListener = (
  config: FullReplicationListenerConfig,
  messageHandler: (
    message: StoredTransactionalMessage,
    cancellation: EventEmitter,
  ) => Promise<void>,
  errorHandler: (
    message: StoredTransactionalMessage,
    err: ExtendedError,
  ) => Promise<boolean>,
  logger: TransactionalLogger,
  strategies: ReplicationStrategies,
): [shutdown: { (): Promise<void> }] => {
  const { dbListenerConfig, settings, outboxOrInbox } = config;
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [settings.dbPublication],
  });
  let client: ReplicationClient | undefined;
  let pool: Pool | undefined;
  let restartTimeout: NodeJS.Timeout | undefined;
  let cleanupTimeout: NodeJS.Timeout | undefined;
  let stopped = false;

  // Run the listener in a self restarting background event "loop" until it gets stopped
  (function start() {
    if (stopped) {
      return;
    }
    logger.debug(`Transactional ${outboxOrInbox} listener starting`);
    restartTimeout = undefined;
    client = new Client({
      ...dbListenerConfig,
      replication: 'database',
      stop: false,
    } as ClientConfig) as ReplicationClient;
    pool = new Pool(dbListenerConfig);

    cleanupTimeout = runScheduledMessageCleanup(pool, config, logger);

    const applyRestart = (promise: Promise<unknown>) => {
      void promise.catch(async (e) => {
        const err = ensureExtendedError(e, 'LISTENER_STOPPED');
        const timeout = await strategies.listenerRestartStrategy(
          err,
          logger,
          outboxOrInbox,
        );
        // On errors stop the DB client and reconnect from a clean state
        await stopClient(client, logger);
        if (!stopped && !restartTimeout) {
          restartTimeout = setTimeout(
            start,
            typeof timeout === 'number' ? timeout : 250,
          );
        }
      });
    };

    // Start background functions to connect the DB client and handle replication data
    applyRestart(
      subscribe(
        client,
        plugin,
        settings.dbReplicationSlot,
        logger,
        outboxOrInbox,
      ),
    );
    applyRestart(
      handleIncomingData(
        client,
        plugin,
        settings,
        messageHandler,
        errorHandler,
        strategies.concurrencyStrategy,
        strategies.messageProcessingTimeoutStrategy,
        logger,
      ),
    );
  })();

  return [
    async (): Promise<void> => {
      logger.debug(`Started transactional ${outboxOrInbox} listener cleanup`);
      stopped = true;
      clearTimeout(restartTimeout);
      clearInterval(cleanupTimeout);
      await stopClient(client, logger);
      await pool?.end();
    },
  ];
};

/** Get and map the outbox/inbox message if the WAL log entry is such a message. Otherwise returns undefined. */
const getRelevantMessage = (
  log: Pgoutput.Message,
  { dbSchema, dbTable }: FullReplicationListenerConfig['settings'],
): StoredTransactionalMessage | undefined =>
  log.tag === 'insert' &&
  log.relation.schema === dbSchema &&
  log.relation.name === dbTable
    ? mapMessage(log.new)
    : undefined;

/** Maps the WAL log entry to an outbox or inbox message */
const mapMessage = (input: unknown): StoredTransactionalMessage | undefined => {
  if (typeof input !== 'object' || input === null) {
    return undefined;
  }
  if (
    !('id' in input) ||
    typeof input.id !== 'string' ||
    !('aggregate_type' in input) ||
    typeof input.aggregate_type !== 'string' ||
    !('aggregate_id' in input) ||
    typeof input.aggregate_id !== 'string' ||
    !('message_type' in input) ||
    typeof input.message_type !== 'string' ||
    !('created_at' in input) ||
    !(input.created_at instanceof Date) ||
    !('payload' in input) ||
    !('metadata' in input)
  ) {
    return undefined;
  }
  const message: TransactionalMessage = {
    id: input.id,
    aggregateType: input.aggregate_type,
    aggregateId: input.aggregate_id,
    messageType: input.message_type,
    payload: input.payload,
    metadata: input.metadata as Record<string, unknown> | undefined,
    createdAt: input.created_at.toISOString(),
  };
  if ('segment' in input && typeof input.segment === 'string') {
    message.segment = input.segment;
  }
  if (
    'concurrency' in input &&
    (input.concurrency === 'sequential' || input.concurrency === 'parallel')
  ) {
    message.concurrency = input.concurrency;
  }
  if ('locked_until' in input && input.locked_until instanceof Date) {
    message.lockedUntil = input.locked_until.toISOString();
  }
  return message as StoredTransactionalMessage;
};

const subscribe = async (
  client: ReplicationClient,
  plugin: AbstractPlugin,
  slotName: string,
  logger: TransactionalLogger,
  outboxOrInbox: OutboxOrInbox,
  uptoLsn?: string,
) => {
  const lastLsn = uptoLsn || '0/00000000';

  await client.connect();
  client.on('error', (e) => {
    logger.error(
      ensureExtendedError(e, 'DB_ERROR'),
      `Transactional ${outboxOrInbox} listener DB client error`,
    );
  });
  client.connection.on('error', (e) => {
    logger.error(
      ensureExtendedError(e, 'DB_ERROR'),
      `Transactional ${outboxOrInbox} listener DB connection error`,
    );
  });
  client.connection.once('replicationStart', () => {
    logger.trace(`Transactional ${outboxOrInbox} listener started`);
  });
  client.on('notice', (msg) => {
    logger.trace('raised notice', msg.message);
  });

  return plugin.start(client, slotName, lastLsn);
};

const stopClient = async (
  client: ReplicationClient | undefined,
  logger: TransactionalLogger,
) => {
  if (!client) {
    return;
  }
  try {
    client.connection?.removeAllListeners();
    client.removeAllListeners();
    await awaitWithTimeout(
      async () => client.end(),
      1000,
      `The PostgreSQL client could not be stopped within a reasonable time frame.`,
    );
  } catch (e) {
    logger.warn(
      ensureExtendedError(e, 'DB_ERROR'),
      `Stopping the PostgreSQL client gave an error.`,
    );
  }
};

// The acknowledge function is based on the https://github.com/kibae/pg-logical-replication library
const acknowledge = (
  client: ReplicationClient,
  lsn: string,
  logger: TransactionalLogger,
): void => {
  if (!client?.connection) {
    logger.warn(
      `Could not acknowledge message ${lsn} as the client connection was not open.`,
    );
    return;
  }

  const slice = lsn.split('/');
  let [upperWAL, lowerWAL]: [number, number] = [
    parseInt(slice[0], 16),
    parseInt(slice[1], 16),
  ];

  // Timestamp as microseconds since midnight 2000-01-01
  const now = Date.now() - 946080000000;
  const upperTimestamp = Math.floor(now / 4294967.296);
  const lowerTimestamp = Math.floor(now - upperTimestamp * 4294967.296);

  if (lowerWAL === 4294967295) {
    // [0xff, 0xff, 0xff, 0xff]
    upperWAL = upperWAL + 1;
    lowerWAL = 0;
  } else {
    lowerWAL = lowerWAL + 1;
  }

  const response = Buffer.alloc(34);
  response.fill(0x72); // 'r'

  // Last WAL Byte + 1 received and written to disk locally
  response.writeUInt32BE(upperWAL, 1);
  response.writeUInt32BE(lowerWAL, 5);

  // Last WAL Byte + 1 flushed to disk in the standby
  response.writeUInt32BE(upperWAL, 9);
  response.writeUInt32BE(lowerWAL, 13);

  // Last WAL Byte + 1 applied in the standby
  response.writeUInt32BE(upperWAL, 17);
  response.writeUInt32BE(lowerWAL, 21);

  // Timestamp as microseconds since midnight 2000-01-01
  response.writeUInt32BE(upperTimestamp, 25);
  response.writeUInt32BE(lowerTimestamp, 29);

  // If 1, requests server to respond immediately - can be used to verify connectivity
  response.writeInt8(0, 33);

  client.connection.sendCopyFromChunk(response);
};

const LOG_FLAG = 0x77; // 119 in base 10
const KEEP_ALIVE_FLAG = 0x6b; // 107 in base 10
const handleIncomingData = (
  client: ReplicationClient,
  plugin: AbstractPlugin,
  config: FullReplicationListenerSettings,
  messageHandler: (
    message: StoredTransactionalMessage,
    cancellation: EventEmitter,
  ) => Promise<void>,
  errorHandler: (
    message: StoredTransactionalMessage,
    err: ExtendedError,
  ) => Promise<boolean>,
  concurrencyStrategy: ReplicationConcurrencyController,
  messageProcessingTimeoutStrategy: MessageProcessingTimeoutStrategy,
  logger: TransactionalLogger,
): Promise<void> => {
  if (!client.connection) {
    throw new TransactionalOutboxInboxError(
      'Client not connected.',
      'DB_ERROR',
    );
  }
  const ack = (lsn: string): void => {
    acknowledge(client, lsn, logger);
  };
  const { startProcessingLSN, finishProcessingLSN } = createAcknowledgeManager(
    ack,
    logger,
  );
  let stopped = false;

  // This function handles the message processing and message error handler.
  // It is inlined to stop processing when "stopped" is true.
  const handleOutboxInboxMessage = async (
    messageHandler: (
      message: StoredTransactionalMessage,
      cancellation: EventEmitter,
    ) => Promise<void>,
    errorHandler: (
      message: StoredTransactionalMessage,
      err: ExtendedError,
    ) => Promise<boolean>,
    message: StoredTransactionalMessage,
    lsn: string,
    finishProcessingLSN: (lsn: string) => void,
    logger: TransactionalLogger,
  ) => {
    const cancellation = new EventEmitter();
    try {
      const messageProcessingTimeoutInMs =
        messageProcessingTimeoutStrategy(message);
      await awaitWithTimeout(
        async () => {
          // Need to double check for stopped for concurrency reasons when a message was awaiting the
          // `awaitWithTimeout` function and another message threw an error which should stop this message.
          if (stopped) {
            logger.trace(`Received LSN ${lsn} after the process stopped.`);
            return;
          }
          logger.debug(
            message,
            `Executing the message handler for LSN ${lsn}.`,
          );
          await messageHandler(message, cancellation);
          finishProcessingLSN(lsn);
        },
        messageProcessingTimeoutInMs,
        `Could not process the message with ID ${message.id} and LSN ${lsn} within the timeout of ${messageProcessingTimeoutInMs} milliseconds. Please consider to use a background worker for long running tasks to not block the message processing.`,
      );
      logger.trace(
        message,
        `Finished processing LSN ${lsn} with message id ${message.id} and type ${message.messageType}.`,
      );
    } catch (e) {
      const err = ensureExtendedError(e, 'MESSAGE_HANDLING_FAILED');
      if (err.errorCode === 'TIMEOUT') {
        cancellation.emit('timeout', err);
      }
      const shouldRetry = await errorHandler(message, err);
      if (!shouldRetry) {
        finishProcessingLSN(lsn);
        return;
      }
      throw new MessageError(
        `An error ocurred while handling the message with ID ${message.id} and LSN ${lsn}`,
        'MESSAGE_HANDLING_FAILED',
        message,
        err,
      );
    }
  };

  return new Promise((_resolve, reject) => {
    // The copyData part is based on the https://github.com/kibae/pg-logical-replication library
    client.connection.on(
      'copyData',
      async ({
        chunk: buffer,
      }: {
        length: number;
        chunk: Buffer;
        name: string;
      }) => {
        try {
          if (buffer[0] !== LOG_FLAG && buffer[0] !== KEEP_ALIVE_FLAG) {
            logger.warn({ bufferStart: buffer[0] }, 'Unknown message');
            return;
          }
          const lsn =
            buffer.readUInt32BE(1).toString(16).toUpperCase() +
            '/' +
            buffer.readUInt32BE(5).toString(16).toUpperCase();
          if (stopped) {
            logger.trace(`Received LSN ${lsn} after the process stopped.`);
            return;
          }

          if (buffer[0] === LOG_FLAG) {
            const xLogData = plugin.parse(buffer.subarray(25));
            const message = getRelevantMessage(xLogData, config);
            if (!message) {
              return;
            }
            logger.trace(
              message,
              `Parsed the message for the LSN ${lsn} with message id ${message.id} and type ${message.messageType}. Potentially waiting for mutex.`,
            );
            const release = await concurrencyStrategy.acquire(message);
            startProcessingLSN(lsn); // finish is called in the `handleOutboxInboxMessage` (if it doesn't throw an Error)
            logger.trace(
              message,
              `Started processing LSN ${lsn} with message id ${message.id} and type ${message.messageType}.`,
            );
            try {
              await handleOutboxInboxMessage(
                messageHandler,
                errorHandler,
                message,
                lsn,
                finishProcessingLSN,
                logger,
              );
            } finally {
              release();
            }
          } else if (buffer[0] === KEEP_ALIVE_FLAG) {
            // Primary keep alive message
            const shouldRespond = !!buffer.readInt8(17);
            if (shouldRespond) {
              startProcessingLSN(lsn);
              finishProcessingLSN(lsn);
            }
          }
        } catch (e) {
          stopped = true;
          client.connection.removeAllListeners();
          concurrencyStrategy.cancel();
          reject(e);
        }
      },
    );
  });
};

/**
 * This export is _only_ done for unit tests as the createLogicalReplicationListener
 * function is otherwise very hard to unit test. Exports work only for jest tests!
 */
export const __only_for_unit_tests__: {
  getRelevantMessage?: typeof getRelevantMessage;
  mapMessage?: typeof mapMessage;
} = {};
if (process.env.JEST_WORKER_ID) {
  __only_for_unit_tests__.getRelevantMessage = getRelevantMessage;
  __only_for_unit_tests__.mapMessage = mapMessage;
}

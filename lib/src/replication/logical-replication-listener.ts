import { Client, ClientConfig, Connection } from 'pg';
import { Pgoutput, PgoutputPlugin } from 'pg-logical-replication';
import { AbstractPlugin } from 'pg-logical-replication/dist/output-plugins/abstract.plugin';
import { ErrorType, MessageError, ensureError } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { OutboxMessage } from '../common/message';
import { awaitWithTimeout } from '../common/utils';
import { ConcurrencyController } from '../concurrency-controller/concurrency-controller';
import { createAcknowledgeManager } from './acknowledge-manager';
import {
  ReplicationListenerConfig,
  TransactionalOutboxInboxConfig,
} from './config';

/** connection exists on the Client but is not typed */
type ReplicationClient = Client & {
  connection: Connection & { sendCopyFromChunk: (buffer: Buffer) => void };
};

/**
 * Initiate the outbox/inbox table to listen for WAL messages.
 * @param config The replication connection settings and general replication settings
 * @param messageHandler The message handler that handles the outbox/inbox message
 * @param errorHandler A handler that can decide if the WAL message should be acknowledged (permanent_error) or not (transient_error which restarts the logical replication listener)
 * @param concurrencyController A controller that ensures some concurrency guarantees.
 * @param logger A logger instance for logging trace up to error logs
 * @param mapAdditionalRows The inbox table requires an additional row to be mapped to the inbox message
 * @returns A function to stop the logical replication listener
 */
export const createLogicalReplicationListener = <T extends OutboxMessage>(
  { pgReplicationConfig, settings }: TransactionalOutboxInboxConfig,
  messageHandler: (message: T) => Promise<void>,
  errorHandler: (message: T, err: Error) => Promise<ErrorType>,
  concurrencyController: ConcurrencyController,
  logger: TransactionalLogger,
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
): [shutdown: { (): Promise<void> }] => {
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [settings.postgresPub],
  });
  let client: ReplicationClient | undefined;
  let restartTimeout: NodeJS.Timeout | undefined;
  let stopped = false;
  // Run the listener in a self restarting background event "loop" until it gets stopped
  (function start() {
    if (stopped) {
      return;
    }
    logger.debug(`Transactional outbox/inbox listener starting`);
    restartTimeout = undefined;
    client = new Client({
      ...pgReplicationConfig,
      replication: 'database',
      stop: false,
    } as ClientConfig) as ReplicationClient;
    // On errors stop the DB client and reconnect from a clean state
    const applyRestart = (promise: Promise<unknown>) => {
      promise
        .catch((e) => {
          const err = ensureError(e);
          if (!(err instanceof MessageError)) {
            logger.error(err, `Transactional outbox/inbox listener error`);
          }
          return err;
        })
        .then((err) => {
          stopClient(client, logger);
          return err;
        })
        .then((err) => {
          if (!stopped && !restartTimeout) {
            restartTimeout = setTimeout(
              start,
              getRestartTimeout(err, settings),
            );
          }
        });
    };

    // Start background functions to connect the DB client and handle replication data
    applyRestart(subscribe(client, plugin, settings.postgresSlot, logger));
    applyRestart(
      handleIncomingData(
        client,
        plugin,
        settings,
        messageHandler,
        errorHandler,
        concurrencyController,
        logger,
        mapAdditionalRows,
      ),
    );
  })();

  return [
    async (): Promise<void> => {
      logger.debug('Started transactional outbox/inbox listener cleanup');
      stopped = true;
      if (restartTimeout) {
        clearTimeout(restartTimeout);
      }
      await stopClient(client, logger);
    },
  ];
};

/** Get and map the outbox/inbox message if the WAL log entry is such a message. Otherwise returns undefined. */
const getRelevantMessage = <T extends OutboxMessage>(
  log: Pgoutput.Message,
  { dbSchema, dbTable }: TransactionalOutboxInboxConfig['settings'],
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
): T | undefined =>
  log.tag === 'insert' &&
  log.relation.schema === dbSchema &&
  log.relation.name === dbTable
    ? mapMessage(log.new, mapAdditionalRows)
    : undefined;

/** Maps the WAL log entry to an outbox or inbox message */
const mapMessage = <T extends OutboxMessage>(
  input: unknown,
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
): T | undefined => {
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
    !(input.created_at instanceof Date) || // date
    !('payload' in input) ||
    !('metadata' in input)
  ) {
    return undefined;
  }
  const additional = mapAdditionalRows?.(input);
  const message: OutboxMessage = {
    id: input.id,
    aggregateType: input.aggregate_type,
    aggregateId: input.aggregate_id,
    messageType: input.message_type,
    payload: input.payload,
    metadata: input.metadata as Record<string, unknown> | undefined,
    createdAt: input.created_at.toISOString(),
    ...additional,
  };
  return message as T;
};

const subscribe = async (
  client: ReplicationClient,
  plugin: AbstractPlugin,
  slotName: string,
  logger: TransactionalLogger,
  uptoLsn?: string,
) => {
  const lastLsn = uptoLsn || '0/00000000';

  await client.connect();
  client.on('error', (e) => {
    logger.error(e, `Transactional outbox/inbox listener DB client error`);
  });
  client.connection.on('error', (e) => {
    logger.error(e, `Transactional outbox/inbox listener DB connection error`);
  });
  client.connection.once('replicationStart', () => {
    logger.trace(`Transactional outbox/inbox listener started`);
  });

  return plugin.start(client, slotName, lastLsn);
};

async function handleOutboxInboxMessage<T extends OutboxMessage>(
  settings: ReplicationListenerConfig,
  messageHandler: (message: T) => Promise<void>,
  errorHandler: ((message: T, err: Error) => Promise<ErrorType>) | undefined,
  message: T,
  lsn: string,
  finishProcessingLSN: (lsn: string) => void,
  logger: TransactionalLogger,
) {
  try {
    logger.debug(message, `Executing the message handler for LSN ${lsn}.`);
    await messageHandler(message as T);
    finishProcessingLSN(lsn);
  } catch (e) {
    const err = ensureError(e);
    if (errorHandler) {
      const errorType = await errorHandler(message, err);
      if (errorType === 'permanent_error') {
        finishProcessingLSN(lsn);
        return;
      }
    }
    throw new MessageError(
      `An error ocurred while handling the message with ID ${message.id} and LSN ${lsn}`,
      message,
      err,
    );
  }
}

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
      () => client.end(),
      1000,
      `The PostgreSQL client could not be stopped within a reasonable time frame.`,
    );
  } catch (e) {
    logger.warn(e, `Stopping the PostgreSQL client gave an error.`);
  }
};

const getRestartTimeout = (
  error: unknown,
  config: ReplicationListenerConfig,
): number => {
  const err = error as Error & { code?: string; routine?: string };
  if (err?.code === '55006' && err?.routine === 'ReplicationSlotAcquire') {
    return config.restartDelaySlotInUse ?? 10_000;
  } else {
    return config.restartDelay ?? 250;
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

// The acknowledge function is based on the https://github.com/kibae/pg-logical-replication library
// but with an async loop and custom concurrency handling
const LOG_FLAG = 0x77; // 119 in base 10
const KEEP_ALIVE_FLAG = 0x6b; // 107 in base 10
const handleIncomingData = <T extends OutboxMessage>(
  client: ReplicationClient,
  plugin: AbstractPlugin,
  config: ReplicationListenerConfig,
  messageHandler: (message: T) => Promise<void>,
  errorHandler: (message: T, err: Error) => Promise<ErrorType>,
  concurrencyController: ConcurrencyController,
  logger: TransactionalLogger,
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
): Promise<void> => {
  if (!client.connection) {
    throw new Error('Client not connected.');
  }
  const ack = (lsn: string): void => {
    acknowledge(client, lsn, logger);
  };
  const { startProcessingLSN, finishProcessingLSN } = createAcknowledgeManager(
    ack,
    logger,
  );
  let stopped = false;
  return new Promise((_resolve, reject) => {
    const messageProcessingTimeout = config.messageProcessingTimeout ?? 15_000;
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
            const message = getRelevantMessage<T>(
              xLogData,
              config,
              mapAdditionalRows,
            );
            if (!message) {
              return;
            }
            logger.trace(
              message,
              `Parsed the message for the LSN ${lsn} with message id ${message.id} and type ${message.messageType}. Potentially waiting for mutex.`,
            );
            const release = await concurrencyController.acquire(message);
            startProcessingLSN(lsn); // finish is called in the `handleOutboxInboxMessage` (if it doesn't throw an Error)
            logger.trace(
              message,
              `Started processing LSN ${lsn} with message id ${message.id} and type ${message.messageType}.`,
            );
            try {
              await awaitWithTimeout(
                async () => {
                  // Need to double check for stopped for concurrency reasons when a message was awaiting the
                  // `awaitWithTimeout` function and another message threw an error which should stop this message.
                  if (stopped) {
                    logger.trace(
                      `Received LSN ${lsn} after the process stopped.`,
                    );
                    return;
                  }
                  await handleOutboxInboxMessage<T>(
                    config,
                    messageHandler,
                    errorHandler,
                    message,
                    lsn,
                    finishProcessingLSN,
                    logger,
                  );
                },
                messageProcessingTimeout,
                `Could not process the message within the timeout of ${messageProcessingTimeout} milliseconds. Please consider to use a background worker for long running tasks to not block the message processing.`,
              );
              logger.trace(
                message,
                `Finished processing LSN ${lsn} with message id ${message.id} and type ${message.messageType}.`,
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
          concurrencyController.cancel();
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

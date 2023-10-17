import { on } from 'events';
import { Client, ClientConfig, Connection } from 'pg';
import { Mutex, withTimeout } from 'async-mutex';
import { Pgoutput, PgoutputPlugin } from 'pg-logical-replication';
import { AbstractPlugin } from 'pg-logical-replication/dist/output-plugins/abstract.plugin';
import { logger } from './logger';
import { OutboxMessage } from './models';
import { awaitWithTimeout, ensureError } from './utils';

export interface ServiceConfig {
  /**
   * The "pg" library based settings to initialize the PostgreSQL connection for
   * the logical replication service (with replication permissions)
   */
  pgReplicationConfig: ClientConfig;
  /** service specific configurations */
  settings: ServiceConfigSettings;
}

export interface ServiceConfigSettings {
  /** The database schema name where the table is located */
  dbSchema: string;
  /** The database table of the inbox/outbox */
  dbTable: string;
  /** The name of the used PostgreSQL publication */
  postgresPub: string;
  /** The name of the used PostgreSQL logical replication slot */
  postgresSlot: string;
  /** When there is a message processing error it restarts the logical replication subscription with a delay. This setting defines this delay in milliseconds. Default is 250ms. */
  restartDelay?: number;
  /** When the replication slot is in use e.g. by another service, this service will still continue to try to connect in case the other service stops. Delay is given in milliseconds, the default is 10s. */
  restartDelaySlotInUse?: number;
  /** Message handlers that do not finish can block further messages from being processed. The timeout (in milliseconds) ensures to continue with the next items. Default is 15s. */
  messageHandlerTimeout?: number;
}

/** connection exists on the Client but is not typed */
type ReplicationClient = Client & {
  connection: Connection & { sendCopyFromChunk: (buffer: Buffer) => void };
};

/**
 * Initiate the outbox/inbox table to listen for WAL messages.
 * @param config The replication connection settings and general service settings
 * @param messageHandler The message handler that handles the outbox/inbox message
 * @param errorHandler A handler that can decide if the WAL message should be acknowledged (true) or not (restarts the logical replication service)
 * @param mapAdditionalRows The inbox table requires an additional row to be mapped to the inbox message
 * @returns A function to stop the service
 */
export const createService = <T extends OutboxMessage>(
  { pgReplicationConfig, settings }: ServiceConfig,
  messageHandler: (message: T) => Promise<void>,
  errorHandler?: (message: T, err: Error) => Promise<void>,
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
): [shutdown: { (): Promise<void> }] => {
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [settings.postgresPub],
  });
  let client: ReplicationClient | undefined;
  let restartTimeout: NodeJS.Timeout | undefined;
  let stopped = false;
  // Run the service in a self restarting background event "loop" until it gets stopped
  (function start() {
    if (stopped) {
      return;
    }
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
          logger().error(err);
          return err;
        })
        .then((err) => {
          stopClient(client);
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
    applyRestart(subscribe(client, plugin, settings.postgresSlot));
    applyRestart(
      handleIncomingData(
        client,
        plugin,
        settings,
        messageHandler,
        errorHandler,
        mapAdditionalRows,
      ),
    );
  })();

  return [
    async (): Promise<void> => {
      logger().debug('started cleanup');
      stopped = true;
      if (restartTimeout) {
        clearTimeout(restartTimeout);
      }
      await stopClient(client);
    },
  ];
};

/** Get and map the inbox/outbox message if the WAL log entry is such a message. Otherwise returns undefined. */
const getRelevantMessage = <T extends OutboxMessage>(
  log: Pgoutput.Message,
  { dbSchema, dbTable }: ServiceConfig['settings'],
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
  uptoLsn?: string,
) => {
  const lastLsn = uptoLsn || '0/00000000';

  await client.connect();
  client.on('error', (e) => {
    logger().error(e, 'client error');
  });
  client.connection.on('error', (e) => {
    logger().error(e, 'connection error');
  });
  client.connection.once('replicationStart', () => {
    logger().trace('Replication started');
  });

  return plugin.start(client, slotName, lastLsn);
};

async function handleInboxOutboxMessage<T extends OutboxMessage>(
  client: ReplicationClient,
  settings: ServiceConfigSettings,
  messageHandler: (message: T) => Promise<void>,
  errorHandler: ((message: T, err: Error) => Promise<void>) | undefined,
  mapAdditionalRows: ((row: object) => Record<string, unknown>) | undefined,
  xLogData: Pgoutput.Message,
  lsn: string,
) {
  const message = getRelevantMessage<T>(xLogData, settings, mapAdditionalRows);
  if (!message) {
    acknowledge(client, lsn); // acknowledge messages that the service cannot use
    return;
  }

  try {
    logger().debug(
      message,
      `Received a WAL message for ${settings.dbSchema}.${settings.dbTable}`,
    );
    await messageHandler(message as T);
    acknowledge(client, lsn);
  } catch (e) {
    logger().error(xLogData, 'Error handling and acknowledging message');
    const err = ensureError(e);
    if (errorHandler) {
      await errorHandler(message, err);
    }
    throw e;
  }
}

const stopClient = async (client: ReplicationClient | undefined) => {
  if (!client) {
    return;
  }
  try {
    client.connection?.removeAllListeners();
    client.removeAllListeners();
    await awaitWithTimeout(
      () => client.end(),
      1000,
      `PostgreSQL client could not be stopped within a reasonable time frame.`,
    );
  } catch (e) {
    logger().warn(e, 'Stopping the PostgreSQL client gave an error.');
  }
};

const getRestartTimeout = (
  error: unknown,
  settings: ServiceConfigSettings,
): number => {
  const err = error as Error & { code?: string; routine?: string };
  if (err?.code === '55006' && err?.routine === 'ReplicationSlotAcquire') {
    return settings.restartDelaySlotInUse ?? 10_000;
  } else {
    return settings.restartDelay ?? 250;
  }
};

// The acknowledge function is based on the https://github.com/kibae/pg-logical-replication library
const acknowledge = (client: ReplicationClient, lsn: string): boolean => {
  if (!client?.connection) {
    logger().warn(
      `Could not acknowledge message ${lsn} as the client connection was not open.`,
    );
    return false;
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

  return true;
};

// The acknowledge function is based on the https://github.com/kibae/pg-logical-replication library
// but with an async loop and a mutex to guarantee sequential processing
const LOG_FLAG = 0x77; // 119 in base 10
const KEEP_ALIVE_FLAG = 0x6b; // 107 in base 10
const handleIncomingData = async <T extends OutboxMessage>(
  client: ReplicationClient,
  plugin: AbstractPlugin,
  settings: ServiceConfigSettings,
  messageHandler: (message: T) => Promise<void>,
  errorHandler?: (message: T, err: Error) => Promise<void>,
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
): Promise<void> => {
  if (!client.connection) {
    throw new Error('Client not connected.');
  }
  const mutex = withTimeout(
    new Mutex(),
    settings.messageHandlerTimeout ?? 15_000,
  );
  for await (const [data] of on(client.connection, 'copyData')) {
    const release = await mutex.acquire();
    try {
      // The semaphore ensure the sequential message processing to guarantee the correct order
      const {
        chunk: buffer,
      }: {
        length: number;
        chunk: Buffer;
        name: string;
      } = data;
      if (buffer[0] !== LOG_FLAG && buffer[0] !== KEEP_ALIVE_FLAG) {
        logger().warn({ bufferStart: buffer[0] }, 'Unknown message');
        return;
      }
      const lsn =
        buffer.readUInt32BE(1).toString(16).toUpperCase() +
        '/' +
        buffer.readUInt32BE(5).toString(16).toUpperCase();

      if (buffer[0] === LOG_FLAG) {
        const xLogData = plugin.parse(buffer.subarray(25));
        await handleInboxOutboxMessage<T>(
          client,
          settings,
          messageHandler,
          errorHandler,
          mapAdditionalRows,
          xLogData,
          lsn,
        );
      } else if (buffer[0] === KEEP_ALIVE_FLAG) {
        // Primary keep alive message
        const shouldRespond = !!buffer.readInt8(17);
        if (shouldRespond) {
          acknowledge(client, lsn);
        }
      }
    } finally {
      release();
    }
  }
};

/**
 * This export is _only_ done for unit tests as the createService function is
 * otherwise very hard to unit test. Exports work only for jest tests!
 */
export const __only_for_unit_tests__: {
  getRelevantMessage?: typeof getRelevantMessage;
  mapMessage?: typeof mapMessage;
} = {};
if (process.env.JEST_WORKER_ID) {
  __only_for_unit_tests__.getRelevantMessage = getRelevantMessage;
  __only_for_unit_tests__.mapMessage = mapMessage;
}

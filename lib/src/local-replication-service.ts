import { ClientConfig } from 'pg';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import { logger } from './logger';
import { ensureError } from './utils';

/** The outbox message for storing it to the DB and receiving it back from the WAL */
export interface OutboxMessage {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  payload: unknown;
  createdAt: string;
}

export interface ServiceConfig {
  /**
   * The "pg" library based settings to initialize the PostgreSQL connection for
   * the logical replication service (with replication permissions)
   */
  pgReplicationConfig: ClientConfig;
  /** service specific configurations */
  settings: {
    /** The database schema name where the table is located */
    dbSchema: string;
    /** The database table of the inbox/outbox */
    dbTable: string;
    /** The name of the used PostgreSQL replication */
    postgresPub: string;
    /** The name of the used PostgreSQL logical replication slot */
    postgresSlot: string;
  };
}

export const createService = async <T extends OutboxMessage>(
  config: ServiceConfig,
  messageHandler: (message: T) => Promise<void>,
  errorHandler?: (
    err: Error,
    message: T,
    ack: () => Promise<void>,
  ) => Promise<void>,
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
): Promise<[shutdown: { (): Promise<void> }]> => {
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [config.settings.postgresPub],
  });

  const create = () =>
    createServiceInstance(
      config,
      errorListener,
      messageHandler,
      errorHandler,
      mapAdditionalRows,
    );

  const errorListener = async (err: Error) => {
    logger().error(err);
    // Stop the current instance and create a new instance e.g. if the DB connection failed
    await service.stop();
    service = create();
  };

  let service = create();

  const flags = { wasShutDown: false };
  const subscribeToOutboxMessages = (): void => {
    service
      // `.subscribe` will start the replication and continue to listen until it is stopped
      .subscribe(plugin, config.settings.postgresSlot)
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
  await service.waitFor('start', 10000);
  return [
    async () => {
      flags.wasShutDown = true;
      service.removeAllListeners();
      service.stop().catch((e) => logger().error(e));
    },
  ];
};

const createServiceInstance = <T extends OutboxMessage>(
  config: ServiceConfig,
  errorListener: (err: Error) => Promise<void>,
  messageHandler: (message: T) => Promise<void>,
  errorHandler?: (
    err: Error,
    message: T,
    ack: () => Promise<void>,
  ) => Promise<void>,
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
) => {
  const service = new LogicalReplicationService(config.pgReplicationConfig, {
    acknowledge: { auto: false, timeoutSeconds: 0 },
  });
  service.on('data', async (lsn: string, log: Pgoutput.Message) => {
    await onData(
      log,
      async () => {
        await service.acknowledge(lsn);
      },
      config.settings,
      messageHandler,
      errorHandler,
      mapAdditionalRows,
    );
  });
  service.on('error', errorListener);
  return service;
};

const onData = async <T extends OutboxMessage>(
  message: Pgoutput.Message,
  ack: () => Promise<void>,
  settings: ServiceConfig['settings'],
  messageHandler: (message: T) => Promise<void>,
  errorHandler?: (
    err: Error,
    message: T,
    ack: () => Promise<void>,
  ) => Promise<void>,
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
) => {
  const msg = getRelevantMessage(message, settings, mapAdditionalRows);
  if (msg) {
    // 'OutboxMessage' is assignable to the constraint of type 'T',
    // but 'T' could be instantiated with a different subtype of constraint 'OutboxMessage'
    const message = msg as T;
    logger().trace(
      message,
      `Received a WAL message for ${settings.dbSchema}.${settings.dbTable}`,
    );
    try {
      await messageHandler(message);
      await ack();
    } catch (err) {
      const error = ensureError(err);
      logger().error({ ...message, err: error }, error.message);
      try {
        if (errorHandler) {
          await errorHandler(error, message, async () => {
            await ack();
          });
        }
      } catch (error) {
        logger().error(
          { ...message, err: error },
          'The errorHandler handling failed.',
        );
      }
    }
  }
};

const getRelevantMessage = <T extends OutboxMessage>(
  log: Pgoutput.Message,
  { dbSchema, dbTable }: ServiceConfig['settings'],
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
): T | undefined =>
  log.tag === 'insert' &&
  log.relation.schema === dbSchema &&
  log.relation.name === dbTable
    ? getOutboxMessage(log.new, mapAdditionalRows)
    : undefined;

const getOutboxMessage = <T extends OutboxMessage>(
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
    !('event_type' in input) ||
    typeof input.event_type !== 'string' ||
    !('created_at' in input) ||
    !(input.created_at instanceof Date) || // date
    !('payload' in input)
  ) {
    return undefined;
  }
  const additional = mapAdditionalRows?.(input);
  const message: OutboxMessage = {
    id: input.id,
    aggregateType: input.aggregate_type,
    aggregateId: input.aggregate_id,
    eventType: input.event_type,
    payload: input.payload,
    createdAt: input.created_at.toISOString(),
    ...additional,
  };
  return message as T;
};

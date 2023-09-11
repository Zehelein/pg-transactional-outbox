import { ClientConfig } from 'pg';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import { logger } from './logger';
import { OutboxMessage } from './models';
import { ensureError, sleep } from './utils';

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
    /** When there is a message processing error it restarts the logical replication subscription with a delay. This setting defines this delay in milliseconds. Default is 1000. */
    restartDelay?: number;
  };
}

/**
 * Initiate the outbox/inbox table to listen for WAL messages.
 * @param config The replication connection settings and general service settings
 * @param messageHandler The message handler that handles the outbox/inbox message
 * @param errorHandler A handler that can decide if the WAL message should be acknowledged (true) or not (restarts the logical replication service)
 * @param mapAdditionalRows The inbox table requires an additional row to be mapped to the inbox message
 * @returns A function to stop the service
 */
export const createService = async <T extends OutboxMessage>(
  { pgReplicationConfig, settings }: ServiceConfig,
  messageHandler: (message: T) => Promise<void>,
  errorHandler?: (message: T, err: Error) => Promise<void>,
  mapAdditionalRows?: (row: object) => Record<string, unknown>,
): Promise<[shutdown: { (): Promise<void> }]> => {
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [settings.postgresPub],
  });
  let service: LogicalReplicationService;
  let stopped = false;
  // Run the service in an endless background loop until it gets stopped
  (async () => {
    while (!stopped) {
      try {
        await new Promise((resolve, reject) => {
          let heartbeatAckTimer: NodeJS.Timeout | undefined = undefined;
          service = new LogicalReplicationService(pgReplicationConfig, {
            acknowledge: { auto: false, timeoutSeconds: 0 },
          });
          service.on('data', async (lsn: string, log: Pgoutput.Message) => {
            if (service.isStop()) {
              logger().error(
                'Received data even though the service is stopped',
              );
              return;
            }
            const msg = getRelevantMessage(log, settings, mapAdditionalRows);
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
                clearTimeout(heartbeatAckTimer);
                await service.acknowledge(lsn);
              } catch (error) {
                const err = ensureError(error);
                logger().error({ ...message, err }, err.message);
                if (errorHandler) {
                  await errorHandler(message, err);
                }
                if (!service.isStop()) {
                  service.emit('error', error);
                }
              }
            }
          });
          service.on('error', async (err: Error) => {
            service.removeAllListeners();
            await service.stop();
            reject(err);
          });
          service.on('heartbeat', async (lsn, _timestamp, shouldRespond) => {
            if (shouldRespond) {
              heartbeatAckTimer = setTimeout(async () => {
                logger().trace(`${lsn}: acknowledged heartbeat`);
                await service.acknowledge(lsn);
              }, 5000);
            }
          });
          service.on('start', async () => {
            logger().debug('Logical replication subscription started');
          });
          service
            .subscribe(plugin, settings.postgresSlot)
            .then(() => resolve(true))
            .catch(async (err) => {
              logger().error({ err }, 'Logical replication subscription error');
              service.removeAllListeners();
              await service.stop();
              reject(err);
            });
        });
      } catch (err) {
        await sleep(settings.restartDelay ?? 1000);
        logger().error({ err }, 'LogicalReplicationService error');
      }
    }
  })();
  return [
    async () => {
      logger().error('started cleanup');
      stopped = true;
      service?.removeAllListeners();
      service
        ?.stop()
        .catch((e) => logger().error(e, 'Error on service shutdown.'));
    },
  ];
};

/** Get and map the inbox/outbox message if the WAL log event is such an event. Otherwise returns undefined. */
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

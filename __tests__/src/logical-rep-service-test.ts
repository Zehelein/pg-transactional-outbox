/* eslint-disable no-console */
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import { sleep } from './test-utils';

/**
 * This is a manual test file to connect to the outbox table and check the
 * behavior of the used LogicalReplicationService. To test best ways to restart
 * the service, implement the heartbeat, test with database outages (manually
 * stop the PostgreSQL docker container), and with faulty message sending.
 *
 * Use the following SQL statement to insert a new row into the outbox table:
 * INSERT INTO outbox.outbox (id, aggregate_type, aggregate_id, message_type, payload)
 * VALUES (gen_random_uuid(), 'movie', gen_random_uuid(), 'movie_created', '{"content":"test"}'::jsonb)
 * RETURNING id, created_at;
 *
 * Adjust the configuration settings to the ones of your database:
 */

const inboxServiceConfig = {
  pgReplicationConfig: {
    host: 'localhost',
    port: 15432,
    database: 'pg_transactional_outbox',
    user: 'db_outbox',
    password: 'db_outbox_password',
  },
  settings: {
    dbSchema: 'outbox',
    dbTable: 'outbox',
    postgresPub: 'pg_transactional_outbox_pub',
    postgresSlot: 'pg_transactional_outbox_slot',
  },
};

const main = async (config: typeof inboxServiceConfig): Promise<void> => {
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [config.settings.postgresPub],
  });

  while (plugin) {
    try {
      await new Promise((resolve, reject) => {
        let heartbeatAckTimer: NodeJS.Timeout | undefined = undefined;
        const service = new LogicalReplicationService(
          config.pgReplicationConfig,
          {
            acknowledge: { auto: false, timeoutSeconds: 0 },
          },
        );
        service.on('data', async (lsn: string, log: Pgoutput.Message) => {
          console.log(
            `                                     | ${lsn} | Log arrived with tag ${log.tag}`,
          );
          if (log.tag === 'insert') {
            console.log(`${log.new.id} | ${lsn} | Outbox message arrived.`);
            if (Math.random() > 0.11) {
              clearTimeout(heartbeatAckTimer);
              await service.acknowledge(lsn);
              console.log(`${log.new.id} | ${lsn} | Acknowledged message.`);
            } else {
              console.log(`${log.new.id} | ${lsn} | Emitting fake error`);
              service.emit(
                'error',
                new Error(`${log.new.id} | ${lsn} | Fake error`),
              );
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
              console.log(`${lsn}: acknowledged heartbeat`);
              await service.acknowledge(lsn);
            }, 5000);
          }
        });

        service
          .subscribe(plugin, config.settings.postgresSlot)
          .then(() => resolve(true))
          .catch(async (err) => {
            console.log(err.message, 'Logical replication subscription error');
            service.removeAllListeners();
            await service.stop();
            reject(err);
          });
      });
    } catch (err) {
      await sleep(1000);
      console.log(err, 'Some error');
    }
  }
};

main(inboxServiceConfig).catch((err) => console.log(err));

// Exit the process if there is an unhandled promise error
process.on('unhandledRejection', (err, promise) => {
  console.error({ err, promise }, 'Unhandled promise rejection');
  process.exit(1);
});

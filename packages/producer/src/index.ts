import path from 'path';
require('dotenv').config({ path: path.join(__dirname, '../.env') });
import { Config, getConfig } from './config';
import { Client } from 'pg';
import {
  LogicalReplicationService,
  PgoutputPlugin,
  Pgoutput,
} from 'pg-logical-replication';
import { v4 as uuid } from 'uuid';

/** Initialize the service to watch for outbox table inserts */
const initializeReplicationService = (
  config: Config,
): LogicalReplicationService => {
  const service = new LogicalReplicationService({
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresOutboxRole,
    password: config.postgresOutboxRolePassword,
    database: config.postgresDatabase,
  });
  service.on('data', (_lsn: string, log: Pgoutput.Message) => {
    if (
      log.tag === 'insert' &&
      log.relation.schema === config.postgresOutboxSchema &&
      log.relation.name === 'outbox'
    ) {
      const {
        id,
        aggregate_type,
        aggregate_id,
        event_type,
        payload,
        created_at,
      } = log.new;
      // TODO: implement the actual sending of the message
      console.log(
        `Sending message with id ${id} for aggregate type ${aggregate_type} with aggregate ID ${aggregate_id} for event of type ${event_type}. The payload is ${JSON.stringify(
          payload,
        )}`,
      );
    }
  });

  service.on('error', (err: Error) => {
    console.error(err);
  });

  return service;
};

/**
 * Start listening to the replication slot and restart the subscription for the
 * PostgreSQL published events if there is an error. It will start the ongoing
 * asynchronous listening.
 * @param service The replication service instance
 */
const ensureReplicationSubscription = (
  service: LogicalReplicationService,
  plugin: PgoutputPlugin,
  slotName: string,
): void => {
  service
    // The subscribe will start the replication and never return unless it is stopped
    .subscribe(plugin, slotName)
    // Log any error and restart the replication after a small timeout
    // The service will catch up with any events in the WAL.
    .catch((e) => {
      console.error(e);
    })
    .then(() => {
      setTimeout(ensureReplicationSubscription, 100);
    });
};

(async () => {
  const config = getConfig();

  // Initialize and start the subscription
  const service = initializeReplicationService(config);
  const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [config.postgresOutboxPub],
  });
  ensureReplicationSubscription(service, plugin, config.postgresOutboxSlot);

  // Produce some fake movie data and outbox events to test the implementation
  const dbClient = new Client({
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresLoginRole,
    password: config.postgresLoginRolePassword,
    database: config.postgresDatabase,
  });
  await dbClient.connect();
  setInterval(async () => {
    // Insert some movie
    const movieInsertedIdResult = await dbClient.query(/*sql*/ `
      INSERT INTO public.movies (title, description, actors, directors, studio)
      VALUES ('some movie', 'some description', ARRAY['Some Actor'], ARRAY['Some Director'], 'Some Studio')
      RETURNING id, title;
    `);

    if (movieInsertedIdResult.rowCount > 0) {
      // If the movie was inserted add the event message to the outbox
      const outboxId = uuid();
      const { id, title } = movieInsertedIdResult.rows[0];
      console.log(`Inserted movie with id ${id}.`);
      await dbClient.query(
        `INSERT INTO ${config.postgresOutboxSchema}.outbox
            (id, aggregate_type, aggregate_id, event_type, payload)
            VALUES ($1, $2, $3, $4, $5);`,
        [outboxId, 'movie', id, 'movie_created', { id, title }],
      );
      // Immediately delete the outbox entry - it was already written to the WAL
      await dbClient.query(
        `DELETE FROM ${config.postgresOutboxSchema}.outbox WHERE id = $1;`,
        [outboxId],
      );
    }

    if (Math.random() > 0.9) {
      // Stop service to fake some subscription outage, still produce videos
      await service.stop();
    }
    if (Math.random() > 0.9 && service.isStop()) {
      // Restart the service, it will now catch up with all outbox messages from the WAL
      ensureReplicationSubscription(service, plugin, config.postgresOutboxSlot);
    }
  }, 1000);
})();

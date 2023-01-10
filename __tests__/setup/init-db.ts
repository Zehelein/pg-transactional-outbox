import { Client, ClientConfig } from 'pg';
import {
  OutboxServiceConfig,
  InboxServiceConfig,
  logger,
} from 'pg-transactional-outbox';
import {
  defaultLoginConnection,
  defaultOutboxServiceConfig,
  defaultInboxServiceConfig,
} from '../src/test-utils/default-configs';

/** Setup on the PostgreSQL server level (and not within a DB) */
const dbmsSetup = async (
  defaultLoginConnection: ClientConfig,
  outSrvConfig: OutboxServiceConfig,
  inSrvConfig: InboxServiceConfig,
): Promise<void> => {
  const { host, port, database, user, password } = defaultLoginConnection;
  const rootClient = new Client({
    host,
    port,
    user: 'postgres',
    password: 'postgres',
  });
  rootClient.connect();
  try {
    await rootClient.query(/*sql*/ `
      SELECT pg_terminate_backend (pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE pg_stat_activity.datname = '${database}';
    `);
    await rootClient.query(/*sql*/ `
      DROP DATABASE IF EXISTS ${database};
    `);
    await rootClient.query(/*sql*/ `
      CREATE DATABASE ${database};
    `);
    await rootClient.query(/*sql*/ `
      DROP ROLE IF EXISTS ${outSrvConfig.pgReplicationConfig.user};
      CREATE ROLE ${outSrvConfig.pgReplicationConfig.user} WITH REPLICATION LOGIN PASSWORD '${outSrvConfig.pgReplicationConfig.password}';
    `);

    await rootClient.query(/*sql*/ `
      DROP ROLE IF EXISTS ${inSrvConfig.pgReplicationConfig.user};
      CREATE ROLE ${inSrvConfig.pgReplicationConfig.user} WITH REPLICATION LOGIN PASSWORD '${inSrvConfig.pgReplicationConfig.password}';
    `);
    await rootClient.query(/*sql*/ `
      DROP ROLE IF EXISTS ${user};
      CREATE ROLE ${user} WITH LOGIN PASSWORD '${password}';
      GRANT CONNECT ON DATABASE ${database} TO ${user};
    `);
    rootClient.end();
    logger().info('Created the database and the outbox, inbox and login roles');
  } catch (err) {
    logger().error(err);
    process.exit(1);
  }
};

const outboxSetup = async (
  defaultLoginConnection: ClientConfig,
  outSrvConfig: OutboxServiceConfig,
): Promise<void> => {
  const { host, port, database, user } = defaultLoginConnection;
  const dbClient = new Client({
    host,
    port,
    database,
    user: 'postgres',
    password: 'postgres',
  });
  dbClient.connect();
  try {
    await dbClient.query(/*sql*/ `
      CREATE SCHEMA IF NOT EXISTS ${outSrvConfig.settings.outboxSchema}
    `);
    await dbClient.query(/*sql*/ `
      DROP TABLE IF EXISTS ${outSrvConfig.settings.outboxSchema}.outbox CASCADE;
      CREATE TABLE ${outSrvConfig.settings.outboxSchema}.outbox (
        id uuid PRIMARY KEY,
        aggregate_type VARCHAR(255) NOT NULL,
        aggregate_id VARCHAR(255) NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      GRANT USAGE ON SCHEMA ${outSrvConfig.settings.outboxSchema} TO ${user} ;
      GRANT SELECT, INSERT, UPDATE, DELETE ON ${outSrvConfig.settings.outboxSchema}.outbox TO ${user};
    `);
    await dbClient.query(/*sql*/ `
      DROP PUBLICATION IF EXISTS ${outSrvConfig.settings.outboxSchema};
      CREATE PUBLICATION ${outSrvConfig.settings.postgresOutboxPub} FOR TABLE ${outSrvConfig.settings.outboxSchema}.outbox WITH (publish = 'insert')
    `);
    await dbClient.query(/*sql*/ `
      select pg_create_logical_replication_slot('${outSrvConfig.settings.postgresOutboxSlot}', 'pgoutput');
    `);
    await dbClient.query(/*sql*/ `
      DROP TABLE IF EXISTS public.source_entities CASCADE;
      CREATE TABLE IF NOT EXISTS public.source_entities (
        id uuid PRIMARY KEY,
        content TEXT NOT NULL
      );
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.source_entities TO ${user};
    `);
    dbClient.end();
    logger().info(
      'Added the outbox table and created the publication for the outbox table',
    );
  } catch (err) {
    logger().error(err);
    process.exit(1);
  }
};

/** All the changes related to the inbox implementation in the database */
const inboxSetup = async (
  defaultLoginConnection: ClientConfig,
  inSrvConfig: InboxServiceConfig,
): Promise<void> => {
  const { host, port, database, user } = defaultLoginConnection;
  const dbClient = new Client({
    host,
    port,
    database,
    user: 'postgres',
    password: 'postgres',
  });
  dbClient.connect();
  try {
    await dbClient.query(/*sql*/ `
      CREATE SCHEMA IF NOT EXISTS ${inSrvConfig.settings.inboxSchema}
    `);
    await dbClient.query(/*sql*/ `
      DROP TABLE IF EXISTS ${inSrvConfig.settings.inboxSchema}.inbox CASCADE;
      CREATE TABLE ${inSrvConfig.settings.inboxSchema}.inbox (
        id uuid PRIMARY KEY,
        aggregate_type VARCHAR(255) NOT NULL,
        aggregate_id VARCHAR(255) NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        processed_at TIMESTAMPTZ,
        retries smallint NOT NULL DEFAULT 0
      );
      GRANT USAGE ON SCHEMA ${inSrvConfig.settings.inboxSchema} TO ${user} ;
      GRANT SELECT, INSERT, UPDATE, DELETE ON ${inSrvConfig.settings.inboxSchema}.inbox TO ${user};
    `);
    await dbClient.query(/*sql*/ `
      DROP PUBLICATION IF EXISTS ${inSrvConfig.settings.postgresInboxPub};
      CREATE PUBLICATION ${inSrvConfig.settings.postgresInboxPub} FOR TABLE ${inSrvConfig.settings.inboxSchema}.inbox WITH (publish = 'insert')
    `);
    await dbClient.query(/*sql*/ `
      select pg_create_logical_replication_slot('${inSrvConfig.settings.postgresInboxSlot}', 'pgoutput');
    `);
    await dbClient.query(/*sql*/ `
      DROP TABLE IF EXISTS public.received_entities CASCADE;
      CREATE TABLE IF NOT EXISTS public.received_entities (
        id uuid PRIMARY KEY,
        content TEXT NOT NULL
      );
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.received_entities TO ${user};
    `);
    dbClient.end();
    logger().info(
      'Added the inbox table and created the publication for the inbox table',
    );
  } catch (err) {
    logger().error(err);
    process.exit(1);
  }
};

(async () => {
  try {
    await dbmsSetup(
      defaultLoginConnection,
      defaultOutboxServiceConfig,
      defaultInboxServiceConfig,
    );
    await outboxSetup(defaultLoginConnection, defaultOutboxServiceConfig);
    await inboxSetup(defaultLoginConnection, defaultInboxServiceConfig);
  } catch (err) {
    logger().error(err);
    process.exit(1);
  }
})();

import { Client, ClientConfig } from 'pg';
import { ReplicationConfig } from 'pg-transactional-outbox';
import { TestConfigs } from './configs';

export const setupTestDb = async ({
  handlerConnection: handlerConnection,
  outboxConfig,
  inboxConfig,
}: TestConfigs): Promise<void> => {
  await dbmsSetup(handlerConnection, outboxConfig, inboxConfig);
  await outboxSetup(handlerConnection, outboxConfig);
  await inboxSetup(handlerConnection, inboxConfig);
};

export const resetReplication = async ({
  handlerConnection: handlerConnection,
  outboxConfig,
  inboxConfig,
}: TestConfigs): Promise<void> => {
  await outboxSetup(handlerConnection, outboxConfig);
  await inboxSetup(handlerConnection, inboxConfig);
};

/** Setup on the PostgreSQL server level (and not within a DB) */
const dbmsSetup = async (
  defaultHandlerConnection: ClientConfig,
  outSrvConfig: ReplicationConfig,
  inSrvConfig: ReplicationConfig,
): Promise<void> => {
  const { host, port, database, user, password } = defaultHandlerConnection;
  const rootClient = new Client({
    host,
    port,
    user: 'postgres',
    password: 'postgres',
  });
  await rootClient.connect();

  await rootClient.query(/* sql */ `
      SELECT pg_terminate_backend (pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE pg_stat_activity.datname = '${database}';
    `);
  await rootClient.query(/* sql */ `
      DROP DATABASE IF EXISTS ${database};
    `);
  await rootClient.query(/* sql */ `
      CREATE DATABASE ${database};
    `);
  await rootClient.query(/* sql */ `
      DROP ROLE IF EXISTS ${outSrvConfig.dbListenerConfig.user};
      CREATE ROLE ${outSrvConfig.dbListenerConfig.user} WITH REPLICATION LOGIN PASSWORD '${outSrvConfig.dbListenerConfig.password}';
    `);

  await rootClient.query(/* sql */ `
      DROP ROLE IF EXISTS ${inSrvConfig.dbListenerConfig.user};
      CREATE ROLE ${inSrvConfig.dbListenerConfig.user} WITH REPLICATION LOGIN PASSWORD '${inSrvConfig.dbListenerConfig.password}';
    `);
  await rootClient.query(/* sql */ `
      DROP ROLE IF EXISTS ${user};
      CREATE ROLE ${user} WITH LOGIN PASSWORD '${password}';
      GRANT CONNECT ON DATABASE ${database} TO ${user};
    `);
  await rootClient.end();
};

const outboxSetup = async (
  defaultHandlerConnection: ClientConfig,
  {
    settings: { dbSchema, dbTable, postgresPub, postgresSlot },
  }: ReplicationConfig,
): Promise<void> => {
  const { host, port, database, user } = defaultHandlerConnection;
  const dbClient = new Client({
    host,
    port,
    database,
    user: 'postgres',
    password: 'postgres',
  });
  await dbClient.connect();

  await dbClient.query(/* sql */ `
      CREATE SCHEMA IF NOT EXISTS ${dbSchema};
      GRANT USAGE ON SCHEMA ${dbSchema} TO ${user};
    `);
  await dbClient.query(/* sql */ `
      DROP TABLE IF EXISTS ${dbSchema}.${dbTable} CASCADE;
      CREATE TABLE ${dbSchema}.${dbTable} (
        id uuid PRIMARY KEY,
        aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        message_type TEXT NOT NULL,
        segment TEXT,
        concurrency TEXT NOT NULL DEFAULT 'sequential',
        payload JSONB NOT NULL,
        metadata JSONB,
        locked_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ,
        abandoned_at TIMESTAMPTZ,
        started_attempts smallint NOT NULL DEFAULT 0,
        finished_attempts smallint NOT NULL DEFAULT 0
      );
      ALTER TABLE ${dbSchema}.${dbTable} ADD CONSTRAINT outbox_concurrency_check
        CHECK (concurrency IN ('sequential', 'parallel'));
      GRANT SELECT, INSERT, UPDATE, DELETE ON ${dbSchema}.${dbTable} TO ${user};
    `);
  await dbClient.query(/* sql */ `
      DROP PUBLICATION IF EXISTS ${postgresPub};
      CREATE PUBLICATION ${postgresPub} FOR TABLE ${dbSchema}.${dbTable} WITH (publish = 'insert')
    `);
  await dbClient.query(/* sql */ `
      select pg_create_logical_replication_slot('${postgresSlot}', 'pgoutput');
    `);
  await dbClient.query(/* sql */ `
      DROP TABLE IF EXISTS public.source_entities CASCADE;
      CREATE TABLE IF NOT EXISTS public.source_entities (
        id uuid PRIMARY KEY,
        content TEXT NOT NULL
      );
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.source_entities TO ${user};
    `);
  await dbClient.end();
};

/** All the changes related to the inbox implementation in the database */
const inboxSetup = async (
  defaultHandlerConnection: ClientConfig,
  {
    settings: { dbSchema, dbTable, postgresPub, postgresSlot },
  }: ReplicationConfig,
): Promise<void> => {
  const { host, port, database, user } = defaultHandlerConnection;
  const dbClient = new Client({
    host,
    port,
    database,
    user: 'postgres',
    password: 'postgres',
  });
  await dbClient.connect();

  await dbClient.query(/* sql */ `
      CREATE SCHEMA IF NOT EXISTS ${dbSchema};
      GRANT USAGE ON SCHEMA ${dbSchema} TO ${user} ;
    `);
  await dbClient.query(/* sql */ `
      DROP TABLE IF EXISTS ${dbSchema}.${dbTable} CASCADE;
      CREATE TABLE ${dbSchema}.${dbTable} (
        id uuid PRIMARY KEY,
        aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        message_type TEXT NOT NULL,
        segment TEXT,
        concurrency TEXT NOT NULL DEFAULT 'sequential',
        payload JSONB NOT NULL,
        metadata JSONB,
        locked_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ,
        abandoned_at TIMESTAMPTZ,
        started_attempts smallint NOT NULL DEFAULT 0,
        finished_attempts smallint NOT NULL DEFAULT 0
      );
      ALTER TABLE ${dbSchema}.${dbTable} ADD CONSTRAINT inbox_concurrency_check
        CHECK (concurrency IN ('sequential', 'parallel'));      
      GRANT SELECT, INSERT, UPDATE, DELETE ON ${dbSchema}.${dbTable} TO ${user};
    `);
  await dbClient.query(/* sql */ `
      DROP PUBLICATION IF EXISTS ${postgresPub};
      CREATE PUBLICATION ${postgresPub} FOR TABLE ${dbSchema}.${dbTable} WITH (publish = 'insert')
    `);
  await dbClient.query(/* sql */ `
      select pg_create_logical_replication_slot('${postgresSlot}', 'pgoutput');
    `);
  await dbClient.query(/* sql */ `
      DROP TABLE IF EXISTS public.received_entities CASCADE;
      CREATE TABLE IF NOT EXISTS public.received_entities (
        id uuid PRIMARY KEY,
        content TEXT NOT NULL
      );
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.received_entities TO ${user};
    `);
  await dbClient.end();
};

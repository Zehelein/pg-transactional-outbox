import { Client, ClientConfig } from 'pg';
import {
  DatabasePollingSetupConfig,
  DatabaseReplicationSetupConfig,
  DatabaseSetup,
  PollingConfig,
  ReplicationConfig,
} from 'pg-transactional-outbox';
import { TestConfigs } from './configs';

const {
  dropAndCreateTable,
  grantPermissions,
  setupReplicationCore,
  setupReplicationSlot,
  createPollingFunction,
  setupPollingIndexes,
} = DatabaseSetup;

export const getDatabaseSetupConfig = ({
  dbListenerConfig,
  settings,
  dbHandlerConfig,
}: ReplicationConfig & PollingConfig): DatabaseReplicationSetupConfig &
  DatabasePollingSetupConfig => {
  return {
    outboxOrInbox: 'outbox',
    database: dbListenerConfig.database!,
    schema: settings.dbSchema,
    table: settings.dbTable,
    listenerRole: dbListenerConfig.user!,
    handlerRole: dbHandlerConfig?.user,
    // Replication
    replicationSlot: settings.postgresSlot,
    publication: settings.postgresPub,
    // Polling
    nextMessagesName: settings.nextMessagesFunctionName,
    nextMessagesSchema: settings.nextMessagesFunctionSchema,
  };
};

export const setupTestDb = async (configs: TestConfigs): Promise<void> => {
  const { handlerConnection, outboxConfig, inboxConfig } = configs;
  await dbmsSetup(handlerConnection, outboxConfig, inboxConfig);
  await resetReplication(configs);
};

export const resetReplication = async ({
  handlerConnection,
  outboxConfig,
  inboxConfig,
}: TestConfigs): Promise<void> => {
  await trxSetup(handlerConnection, getDatabaseSetupConfig(outboxConfig));
  await trxSetup(handlerConnection, getDatabaseSetupConfig(inboxConfig));
  await outboxSetup(handlerConnection);
  await inboxSetup(handlerConnection);
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

const trxSetup = async (
  defaultHandlerConnection: ClientConfig,
  setupConfig: DatabaseReplicationSetupConfig & DatabasePollingSetupConfig,
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

  await dbClient.query(dropAndCreateTable(setupConfig));
  await dbClient.query(grantPermissions(setupConfig));
  await dbClient.query(setupReplicationCore(setupConfig));
  await dbClient.query(setupReplicationSlot(setupConfig));
  await dbClient.query(createPollingFunction(setupConfig));
  await dbClient.query(setupPollingIndexes(setupConfig));

  await dbClient.end();
};

const outboxSetup = async (
  defaultHandlerConnection: ClientConfig,
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
      DROP TABLE IF EXISTS public.received_entities CASCADE;
      CREATE TABLE IF NOT EXISTS public.received_entities (
        id uuid PRIMARY KEY,
        content TEXT NOT NULL
      );
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.received_entities TO ${user};
    `);

  await dbClient.end();
};

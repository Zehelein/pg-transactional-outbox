import path from 'path';
// Load the environment files from the local and parent .env file
import dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '.env') });
dotenv.config({ path: path.join(__dirname, '../.env'), override: true });
// eslint-disable-next-line prettier/prettier
import { Client } from 'pg';
import {
  DatabaseSetup,
  PollingConfig,
  ReplicationConfig,
} from 'pg-transactional-outbox';
import {
  Config,
  getConfig,
  getPollingInboxConfig,
  getReplicationInboxConfig,
} from '../src/config';
import { getLogger } from '../src/logger';

const logger = getLogger();
const {
  dropAndCreateHandlerAndListenerRoles,
  dropAndCreateTable,
  grantPermissions,
  setupReplicationCore,
  setupReplicationSlot,
  createPollingFunction,
  setupPollingIndexes,
} = DatabaseSetup;

/** Setup on the PostgreSQL server level (and not within a DB) */
const dbmsSetup = async (
  config: Config,
  replicationConfig: ReplicationConfig,
): Promise<void> => {
  const rootClient = new Client({
    host: config.postgresHost,
    port: config.postgresPort,
    user: process.env.POSTGRESQL_ROOT_ROLE,
    password: process.env.POSTGRESQL_ROOT_ROLE_PASSWORD,
  });
  rootClient.connect();
  try {
    logger.debug('Drop all connections to the database');
    await rootClient.query(/* sql */ `
      SELECT pg_terminate_backend (pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE pg_stat_activity.datname = '${config.postgresDatabase}';
    `);

    logger.debug('Drop the database if it exists');
    await rootClient.query(/* sql */ `
      DROP DATABASE IF EXISTS ${config.postgresDatabase};
    `);

    logger.debug('Create the database');
    await rootClient.query(/* sql */ `
      CREATE DATABASE ${config.postgresDatabase};
    `);

    logger.debug('Create the database inbox listener and handler role');
    await rootClient.query(
      dropAndCreateHandlerAndListenerRoles(replicationConfig),
    );

    rootClient.end();
    logger.info('Created the database and the inbox and handler roles');
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
};

/** All the changes related to the inbox implementation in the database */
const inboxSetup = async (
  config: Config,
  replicationConfig: ReplicationConfig,
  pollingConfig: PollingConfig,
): Promise<void> => {
  const dbClient = new Client({
    ...replicationConfig.dbListenerConfig,
    user: process.env.POSTGRESQL_ROOT_ROLE,
    password: process.env.POSTGRESQL_ROOT_ROLE_PASSWORD,
  });
  dbClient.connect();
  try {
    logger.debug('Create the inbox table and make sure the DB schema exists');
    await dbClient.query(dropAndCreateTable(replicationConfig));

    logger.debug(
      'Grant table and scheme permissions to the handler and listener role',
    );
    await dbClient.query(grantPermissions(replicationConfig));

    // Normally you would only create replication OR polling functionality
    logger.debug('Create the inbox publication and replication slot');
    await dbClient.query(setupReplicationCore(replicationConfig));
    await dbClient.query(setupReplicationSlot(replicationConfig));

    logger.debug('Create the inbox polling function');
    await dbClient.query(createPollingFunction(pollingConfig));

    logger.debug('Create polling indexes');
    await dbClient.query(setupPollingIndexes(pollingConfig));

    dbClient.end();
    logger.info(
      'Added the inbox table and created the replication and polling functionality',
    );
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
};

/** Test setup with the example of a movie database. */
const testDataSetup = async (config: Config): Promise<void> => {
  const dbClient = new Client({
    host: config.postgresHost,
    port: config.postgresPort,
    user: process.env.POSTGRESQL_ROOT_ROLE,
    password: process.env.POSTGRESQL_ROOT_ROLE_PASSWORD,
    database: config.postgresDatabase,
  });
  dbClient.connect();
  try {
    logger.debug(
      'Create the published movies table and grant permissions to the handler role',
    );
    await dbClient.query(/* sql */ `
      DROP TABLE IF EXISTS public.published_movies CASCADE;
      DROP SEQUENCE IF EXISTS published_movies_id_seq;
      CREATE SEQUENCE published_movies_id_seq;
      CREATE TABLE IF NOT EXISTS public.published_movies (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT NOT NULL
      );
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.published_movies TO ${config.postgresHandlerRole};
      GRANT USAGE ON SEQUENCE published_movies_id_seq TO ${config.postgresHandlerRole};
    `);

    dbClient.end();
    logger.debug(
      'Added the published movies tables and granted access to the handler role',
    );
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
};

(async () => {
  try {
    const config = getConfig();
    const replicationConfig = getReplicationInboxConfig(config);
    const pollingConfig = getPollingInboxConfig(config);
    await dbmsSetup(config, replicationConfig);
    await inboxSetup(config, replicationConfig, pollingConfig);
    await testDataSetup(config);
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
})();

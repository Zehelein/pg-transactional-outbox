import path from 'path';
// Load the environment files from the local and parent .env file
import dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '.env') });
dotenv.config({ path: path.join(__dirname, '../.env'), override: true });
// eslint-disable-next-line prettier/prettier
import { Client } from 'pg';
import {
  DatabasePollingSetupConfig,
  DatabaseReplicationSetupConfig,
  DatabaseSetup,
} from 'pg-transactional-outbox';
import { Config, getConfig, getDatabaseSetupConfig } from '../src/config';
import { getLogger } from '../src/logger';

const logger = getLogger();
const {
  dropAndCreateTable,
  grantPermissions,
  setupReplicationCore,
  setupReplicationSlot,
  createPollingFunction,
  setupPollingIndexes,
} = DatabaseSetup;

/** Setup on the PostgreSQL server level (and not within a DB) */
const dbmsSetup = async (config: Config): Promise<void> => {
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

    logger.debug('Create the database outbox role');
    await rootClient.query(/* sql */ `
      DROP ROLE IF EXISTS ${config.postgresOutboxListenerRole};
      CREATE ROLE ${config.postgresOutboxListenerRole} WITH REPLICATION LOGIN PASSWORD '${config.postgresOutboxListenerRolePassword}';
    `);

    logger.debug('Create the database login role');
    await rootClient.query(/* sql */ `
      DROP ROLE IF EXISTS ${config.postgresHandlerRole};
      CREATE ROLE ${config.postgresHandlerRole} WITH LOGIN PASSWORD '${config.postgresHandlerRolePassword}';
      GRANT CONNECT ON DATABASE ${config.postgresDatabase} TO ${config.postgresHandlerRole};
    `);

    rootClient.end();
    logger.info('Created the database and the outbox and handler roles');
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
};

/** All the changes related to the outbox implementation in the database */
const outboxSetup = async (
  config: Config,
  setupConfig: DatabaseReplicationSetupConfig & DatabasePollingSetupConfig,
): Promise<void> => {
  const dbClient = new Client({
    host: config.postgresHost,
    port: config.postgresPort,
    database: config.postgresDatabase,
    user: process.env.POSTGRESQL_ROOT_ROLE,
    password: process.env.POSTGRESQL_ROOT_ROLE_PASSWORD,
  });
  dbClient.connect();
  try {
    logger.debug('Create the outbox table and make sure the DB schema exists');
    await dbClient.query(dropAndCreateTable(setupConfig));

    logger.debug(
      'Grant table and scheme permissions to the handler and listener role',
    );
    await dbClient.query(grantPermissions(setupConfig));

    // Normally you would only create replication OR polling functionality
    logger.debug('Create the outbox publication and replication slot');
    await dbClient.query(setupReplicationCore(setupConfig));
    await dbClient.query(setupReplicationSlot(setupConfig));

    logger.debug('Create the outbox polling function');
    await dbClient.query(createPollingFunction(setupConfig));

    logger.debug('Create polling indexes');
    await dbClient.query(setupPollingIndexes(setupConfig));

    dbClient.end();
    logger.info(
      'Added the outbox table and created the replication and polling functionality',
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
      'Create the movies table and grant permissions to the handler role',
    );
    await dbClient.query(/* sql */ `
      DROP TABLE IF EXISTS public.movies CASCADE;
      DROP SEQUENCE IF EXISTS movies_id_seq;
      CREATE SEQUENCE movies_id_seq;
      CREATE TABLE IF NOT EXISTS public.movies (
        id INTEGER PRIMARY KEY DEFAULT nextval('movies_id_seq'),
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        actors TEXT[] NOT NULL,
        directors TEXT[] NOT NULL,
        studio TEXT NOT NULL
      );
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.movies TO ${config.postgresHandlerRole};
      GRANT USAGE ON SEQUENCE movies_id_seq TO ${config.postgresHandlerRole};
    `);

    logger.debug('Initialize the movie database with some movies');
    await dbClient.query(/* sql */ `
      INSERT INTO public.movies (title, description, actors, directors, studio)
      VALUES
        ('Inception', 'A thief who steals corporate secrets through use of dream-sharing technology is given the inverse task of planting an idea into the mind of a CEO.', ARRAY['Leonardo DiCaprio', 'Joseph Gordon-Levitt', 'Ellen Page'], ARRAY['Christopher Nolan'], 'Warner Bros. Pictures'),
        ('Interstellar', 'A team of explorers travel through a wormhole in space in an attempt to ensure humanity''s survival.', ARRAY['Matthew McConaughey', 'Anne Hathaway', 'Jessica Chastain'], ARRAY['Christopher Nolan'], 'Paramount Pictures');
    `);

    dbClient.end();
    logger.info(
      'Added the movies tables and granted access to the handler role',
    );
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
};

(async () => {
  try {
    const config = getConfig();
    const setupConfig = getDatabaseSetupConfig(config);
    await dbmsSetup(config);
    await outboxSetup(config, setupConfig);
    await testDataSetup(config);
    process.exit(0);
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
})();

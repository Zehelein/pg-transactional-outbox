import path from 'path';
// Load the environment files from the local and parent .env file
import dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '.env') });
dotenv.config({ path: path.join(__dirname, '../.env'), override: true });
import { Client } from 'pg';
import { Config, getConfig } from '../src/config';
import { logger } from '../src/logger';

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
    await rootClient.query(/* sql*/ `
      SELECT pg_terminate_backend (pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE pg_stat_activity.datname = '${config.postgresDatabase}';
    `);

    logger.debug('Drop the database if it exists');
    await rootClient.query(/* sql*/ `
      DROP DATABASE IF EXISTS ${config.postgresDatabase};
    `);

    logger.debug('Create the database');
    await rootClient.query(/* sql*/ `
      CREATE DATABASE ${config.postgresDatabase};
    `);

    logger.debug('Create the database inbox role');
    await rootClient.query(/* sql*/ `
      DROP ROLE IF EXISTS ${config.postgresInboxRole};
      CREATE ROLE ${config.postgresInboxRole} WITH REPLICATION LOGIN PASSWORD '${config.postgresInboxRolePassword}';
    `);

    logger.debug('Create the database login role');
    await rootClient.query(/* sql*/ `
      DROP ROLE IF EXISTS ${config.postgresLoginRole};
      CREATE ROLE ${config.postgresLoginRole} WITH LOGIN PASSWORD '${config.postgresLoginRolePassword}';
      GRANT CONNECT ON DATABASE ${config.postgresDatabase} TO ${config.postgresLoginRole};
    `);

    logger.debug('Add database extensions');
    await rootClient.query(/* sql*/ `
      CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public; -- used for gin indexes which optimize requests that use LIKE/ILIKE operators, e.g. filter by title
      CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public; -- used for generating UUID values for PK fields
    `);

    rootClient.end();
    logger.info('Created the database and the inbox and login roles');
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
};

/** All the changes related to the inbox implementation in the database */
const inboxSetup = async (config: Config): Promise<void> => {
  const dbClient = new Client({
    host: config.postgresHost,
    port: config.postgresPort,
    user: process.env.POSTGRESQL_ROOT_ROLE,
    password: process.env.POSTGRESQL_ROOT_ROLE_PASSWORD,
    database: config.postgresDatabase,
  });
  dbClient.connect();
  try {
    logger.debug('Make sure the inbox database schema exists');
    await dbClient.query(/* sql*/ `
      CREATE SCHEMA IF NOT EXISTS ${config.postgresInboxSchema}
    `);

    logger.debug('Create the inbox table');
    await dbClient.query(/* sql*/ `
      DROP TABLE IF EXISTS ${config.postgresInboxSchema}.${config.postgresInboxTable} CASCADE;
      CREATE TABLE ${config.postgresInboxSchema}.${config.postgresInboxTable} (
        id uuid PRIMARY KEY,
        aggregate_type VARCHAR(255) NOT NULL,
        aggregate_id VARCHAR(255) NOT NULL, 
        event_type VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        processed_at TIMESTAMPTZ,
        retries smallint NOT NULL DEFAULT 0
      );
      GRANT USAGE ON SCHEMA ${config.postgresInboxSchema} TO ${config.postgresLoginRole} ;
      GRANT SELECT, INSERT, UPDATE, DELETE ON ${config.postgresInboxSchema}.${config.postgresInboxTable} TO ${config.postgresLoginRole};
    `);

    logger.debug('Create the inbox publication');
    await dbClient.query(/* sql*/ `
      DROP PUBLICATION IF EXISTS ${config.postgresInboxPub};
      CREATE PUBLICATION ${config.postgresInboxPub} FOR TABLE ${config.postgresInboxSchema}.${config.postgresInboxTable} WITH (publish = 'insert')
    `);
    await dbClient.query(/* sql*/ `
      select pg_create_logical_replication_slot('${config.postgresInboxSlot}', 'pgoutput');
    `);

    dbClient.end();
    logger.info(
      'Added the inbox table and created the publication for the inbox table',
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
      'Create the published movies table and grant permissions to the login role',
    );
    await dbClient.query(/* sql*/ `
      DROP TABLE IF EXISTS public.published_movies CASCADE;
      DROP SEQUENCE IF EXISTS published_movies_id_seq;
      CREATE SEQUENCE published_movies_id_seq;
      CREATE TABLE IF NOT EXISTS public.published_movies (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT NOT NULL
      );
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.published_movies TO ${config.postgresLoginRole};
      GRANT USAGE ON SEQUENCE published_movies_id_seq TO ${config.postgresLoginRole};;
    `);

    dbClient.end();
    logger.debug(
      'Added the published movies tables and granted access to the login role',
    );
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
};

(async () => {
  try {
    const config = getConfig();
    await dbmsSetup(config);
    await inboxSetup(config);
    await testDataSetup(config);
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
})();

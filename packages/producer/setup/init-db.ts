import path from 'path';
// Load the environment files from the local and parent .env file
import dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '.env') });
dotenv.config({ path: path.join(__dirname, '../.env'), override: true });
import { Config, getConfig } from '../src/config';
import { Client } from 'pg';

/** Setup related to the PostgreSQL server (and not a specific DB) */
const dbmsSetup = async (config: Config): Promise<void> => {
  const rootClient = new Client({
    host: config.postgresHost,
    port: config.postgresPort,
    user: process.env.POSTGRESQL_ROOT_ROLE,
    password: process.env.POSTGRESQL_ROOT_ROLE_PASSWORD,
  });
  rootClient.connect();
  try {
    console.log('Drop all connections to the database');
    await rootClient.query(/*sql*/ `
      SELECT pg_terminate_backend (pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE pg_stat_activity.datname = '${config.postgresDatabase}';
    `);

    console.log('Drop the database if it exists');
    await rootClient.query(/*sql*/ `
      DROP DATABASE IF EXISTS ${config.postgresDatabase};
    `);

    console.log('Create the database');
    await rootClient.query(/*sql*/ `
      CREATE DATABASE ${config.postgresDatabase};
    `);

    console.log('Create the database outbox role');
    await rootClient.query(/*sql*/ `
      DROP ROLE IF EXISTS ${config.postgresOutboxRole};
      CREATE ROLE ${config.postgresOutboxRole} WITH REPLICATION LOGIN PASSWORD '${config.postgresOutboxRolePassword}';
    `);

    console.log('Create the database login role');
    await rootClient.query(/*sql*/ `
      DROP ROLE IF EXISTS ${config.postgresLoginRole};
      CREATE ROLE ${config.postgresLoginRole} WITH LOGIN PASSWORD '${config.postgresLoginRolePassword}';
      GRANT CONNECT ON DATABASE ${config.postgresDatabase} TO ${config.postgresLoginRole};
    `);

    console.log('Add database extensions');
    await rootClient.query(/*sql*/ `
      CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public; -- used for gin indexes which optimize requests that use LIKE/ILIKE operators, e.g. filter by title
      CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public; -- used for generating UUID values for PK fields
    `);

    rootClient.end();
    console.log(
      '\x1b[32m%s\x1b[0m',
      'Created the database and the outbox and login roles',
    );
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
};

/** All the changes related to the outbox implementation in the database */
const outboxSetup = async (config: Config): Promise<void> => {
  const dbClient = new Client({
    host: config.postgresHost,
    port: config.postgresPort,
    user: process.env.POSTGRESQL_ROOT_ROLE,
    password: process.env.POSTGRESQL_ROOT_ROLE_PASSWORD,
    database: config.postgresDatabase,
  });
  dbClient.connect();
  try {
    console.log('Make sure the outbox database schema exists');
    await dbClient.query(/*sql*/ `
      CREATE SCHEMA IF NOT EXISTS ${config.postgresOutboxSchema}
    `);

    console.log('Create the outbox table');
    await dbClient.query(/*sql*/ `
      DROP TABLE IF EXISTS ${config.postgresOutboxSchema}.outbox CASCADE;
      CREATE TABLE ${config.postgresOutboxSchema}.outbox (
        id uuid PRIMARY KEY,
        aggregate_type VARCHAR(255) NOT NULL,
        aggregate_id VARCHAR(255) NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      GRANT USAGE ON SCHEMA ${config.postgresOutboxSchema} TO ${config.postgresLoginRole} ;
      GRANT SELECT, INSERT, UPDATE, DELETE ON ${config.postgresOutboxSchema}.outbox TO ${config.postgresLoginRole};
    `);

    console.log('Create the outbox publication');
    await dbClient.query(/*sql*/ `
      DROP PUBLICATION IF EXISTS ${config.postgresOutboxPub};
      CREATE PUBLICATION ${config.postgresOutboxPub} FOR TABLE ${config.postgresOutboxSchema}.outbox WITH (publish = 'insert')
    `);
    await dbClient.query(/*sql*/ `
      select pg_create_logical_replication_slot('${config.postgresOutboxSlot}', 'pgoutput');
    `);

    dbClient.end();
    console.log(
      '\x1b[32m%s\x1b[0m',
      'Added the outbox table and created the publication for the outbox table',
    );
  } catch (err) {
    console.error(err);
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
    console.log(
      'Create the movies table and grant permissions to the login role',
    );
    await dbClient.query(/*sql*/ `
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
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.movies TO ${config.postgresLoginRole};
      GRANT USAGE ON SEQUENCE movies_id_seq TO ${config.postgresLoginRole};;
    `);

    console.log('Initialize the movie database with some movies');
    await dbClient.query(/*sql*/ `
      INSERT INTO public.movies (title, description, actors, directors, studio)
      VALUES
        ('Inception', 'A thief who steals corporate secrets through use of dream-sharing technology is given the inverse task of planting an idea into the mind of a CEO.', ARRAY['Leonardo DiCaprio', 'Joseph Gordon-Levitt', 'Ellen Page'], ARRAY['Christopher Nolan'], 'Warner Bros. Pictures'),
        ('Interstellar', 'A team of explorers travel through a wormhole in space in an attempt to ensure humanity''s survival.', ARRAY['Matthew McConaughey', 'Anne Hathaway', 'Jessica Chastain'], ARRAY['Christopher Nolan'], 'Paramount Pictures');
    `);

    dbClient.end();
    console.log(
      '\x1b[32m%s\x1b[0m',
      'Added the movies tables and granted access to the login role',
    );
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
};

(async () => {
  try {
    const config = getConfig();
    await dbmsSetup(config);
    await outboxSetup(config);
    await testDataSetup(config);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
})();

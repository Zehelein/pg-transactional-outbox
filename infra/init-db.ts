import path from 'path';
// Load the environment files from the local and source .env file
import dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '.env') });
dotenv.config({ path: path.join(__dirname, '../src/.env'), override: true });
const {
  POSTGRESQL_HOST,
  POSTGRESQL_PORT,
  POSTGRESQL_ROOT,
  POSTGRESQL_ROOT_PASSWORD,
  POSTGRESQL_OWNER,
  POSTGRESQL_OWNER_PASSWORD,
  POSTGRESQL_LOGIN,
  POSTGRESQL_LOGIN_PASSWORD,
} = process.env;
import { Client } from 'pg';

const dbmsSetup = async (): Promise<void> => {
  // Connect to the PostgreSQL server as superuser
  const rootClient = new Client({
    host: POSTGRESQL_HOST,
    port: Number(POSTGRESQL_PORT),
    user: POSTGRESQL_ROOT,
    password: POSTGRESQL_ROOT_PASSWORD,
  });
  rootClient.connect();
  try {
    console.log('Drop all connections to the database');
    await rootClient.query(/*sql*/ `
      SELECT pg_terminate_backend (pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE pg_stat_activity.datname = 'pg_transactional_outbox';
    `);

    console.log('Drop the database if it exists');
    await rootClient.query(/*sql*/ `
      DROP DATABASE IF EXISTS pg_transactional_outbox;
    `);

    console.log('Create the database');
    await rootClient.query(/*sql*/ `
      CREATE DATABASE pg_transactional_outbox;
    `);

    console.log(
      'Create the database owner role and make it the database owner',
    );
    await rootClient.query(/*sql*/ `
      DROP ROLE IF EXISTS ${POSTGRESQL_OWNER};
      CREATE ROLE ${POSTGRESQL_OWNER} WITH LOGIN PASSWORD '${POSTGRESQL_OWNER_PASSWORD}';
      ALTER DATABASE pg_transactional_outbox OWNER TO ${POSTGRESQL_OWNER};
    `);

    console.log('Create the database login role');
    await rootClient.query(/*sql*/ `
      DROP ROLE IF EXISTS ${POSTGRESQL_LOGIN};
      CREATE ROLE ${POSTGRESQL_LOGIN} WITH LOGIN PASSWORD '${POSTGRESQL_LOGIN_PASSWORD}';
    `);

    rootClient.end();
    console.log(
      '\x1b[32m%s\x1b[0m',
      'Created the database and the owner and login roles without errors',
    );
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
};

const databaseSetup = async (): Promise<void> => {
  // Connect to the PostgreSQL pg_transactional_outbox database as superuser
  const dbClient = new Client({
    host: POSTGRESQL_HOST,
    port: Number(POSTGRESQL_PORT),
    user: POSTGRESQL_ROOT,
    password: POSTGRESQL_ROOT_PASSWORD,
    database: 'pg_transactional_outbox',
  });
  dbClient.connect();
  try {
    console.log('Grant permissions for the database login role');
    await dbClient.query(/*sql*/ `
      GRANT CONNECT ON DATABASE pg_transactional_outbox TO ${POSTGRESQL_LOGIN};
      ALTER default privileges IN SCHEMA public GRANT usage, SELECT ON sequences TO ${POSTGRESQL_LOGIN};
      ALTER default privileges IN SCHEMA public GRANT EXECUTE ON functions TO ${POSTGRESQL_LOGIN};
    `);

    console.log('Add database extensions');
    await dbClient.query(/*sql*/ `
      CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public; -- used for gin indexes which optimize requests that use LIKE/ILIKE operators, e.g. filter by title
      CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public; -- used for generating UUID values for PK fields
    `);

    console.log('Create the movies table');
    await dbClient.query(/*sql*/ `
      DROP TABLE IF EXISTS movies CASCADE;
      CREATE TABLE IF NOT EXISTS movies (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        actors TEXT[] NOT NULL,
        directors TEXT[] NOT NULL,
        studio TEXT NOT NULL
      );
      GRANT SELECT, INSERT, UPDATE, DELETE ON movies TO ${POSTGRESQL_LOGIN};
    `);

    console.log('Initialize the movie database with some movies');
    await dbClient.query(/*sql*/ `
      INSERT INTO movies (title, description, actors, directors, studio)
      VALUES
        ('Inception', 'A thief who steals corporate secrets through use of dream-sharing technology is given the inverse task of planting an idea into the mind of a CEO.', ARRAY['Leonardo DiCaprio', 'Joseph Gordon-Levitt', 'Ellen Page'], ARRAY['Christopher Nolan'], 'Warner Bros. Pictures'),
        ('Interstellar', 'A team of explorers travel through a wormhole in space in an attempt to ensure humanity''s survival.', ARRAY['Matthew McConaughey', 'Anne Hathaway', 'Jessica Chastain'], ARRAY['Christopher Nolan'], 'Paramount Pictures');
    `);

    dbClient.end();
    console.log(
      '\x1b[32m%s\x1b[0m',
      'Added extensions, grants, and created tables without errors',
    );
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
};

(async () => {
  try {
    await dbmsSetup();
    await databaseSetup();
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
})();

import path from 'path';
// Load the environment files from the local and parent .env file
import dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '.env') });
dotenv.config({ path: path.join(__dirname, '../.env'), override: true });
// eslint-disable-next-line prettier/prettier
import { Client } from 'pg';
import { Config, getConfig } from '../src/config';
import { getLogger } from '../src/logger';

const logger = getLogger();

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

    logger.debug('Create the database handler role');
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
    logger.debug('Make sure the outbox database schema exists');
    await dbClient.query(/* sql */ `
      CREATE SCHEMA IF NOT EXISTS ${config.postgresOutboxSchema};
      GRANT USAGE ON SCHEMA ${config.postgresOutboxSchema} TO ${config.postgresHandlerRole};
      GRANT USAGE ON SCHEMA ${config.postgresOutboxSchema} TO ${config.postgresOutboxListenerRole};
    `);

    logger.debug('Create the outbox table');
    await dbClient.query(/* sql */ `
      DROP TABLE IF EXISTS ${config.postgresOutboxSchema}.${config.postgresOutboxTable} CASCADE;
      CREATE TABLE ${config.postgresOutboxSchema}.${config.postgresOutboxTable} (
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
      ALTER TABLE ${config.postgresOutboxSchema}.${config.postgresOutboxTable} ADD CONSTRAINT outbox_concurrency_check
        CHECK (concurrency IN ('sequential', 'parallel'));
      GRANT SELECT, INSERT, DELETE ON ${config.postgresOutboxSchema}.${config.postgresOutboxTable} TO ${config.postgresHandlerRole};
      GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON ${config.postgresOutboxSchema}.${config.postgresOutboxTable} TO ${config.postgresHandlerRole};
      GRANT SELECT, INSERT, UPDATE, DELETE ON ${config.postgresOutboxSchema}.${config.postgresOutboxTable} TO ${config.postgresOutboxListenerRole};
    `);

    logger.debug('Create the outbox publication');
    await dbClient.query(/* sql */ `
      DROP PUBLICATION IF EXISTS ${config.postgresOutboxPub};
      CREATE PUBLICATION ${config.postgresOutboxPub} FOR TABLE ${config.postgresOutboxSchema}.${config.postgresOutboxTable} WITH (publish = 'insert')
    `);
    await dbClient.query(/* sql */ `
      select pg_create_logical_replication_slot('${config.postgresOutboxSlot}', 'pgoutput');
    `);

    logger.debug('Create the outbox polling function');
    await dbClient.query(/* sql */ `
      DROP FUNCTION IF EXISTS ${config.postgresOutboxSchema}.${config.nextOutboxMessagesFunctionName}(integer);
      CREATE OR REPLACE FUNCTION ${config.postgresOutboxSchema}.${config.nextOutboxMessagesFunctionName}(
        max_size integer)
          RETURNS SETOF ${config.postgresOutboxSchema}.${config.postgresOutboxTable} 
          LANGUAGE 'plpgsql'

      AS $BODY$
      DECLARE 
        loop_row ${config.postgresOutboxSchema}.${config.postgresOutboxTable}%ROWTYPE;
        message_row ${config.postgresOutboxSchema}.${config.postgresOutboxTable}%ROWTYPE;
        ids uuid[] := '{}';
      BEGIN

        IF max_size < 1 THEN
          RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
        END IF;

        -- get (only) the oldest message of every segment - but only return it if it is not locked
        FOR loop_row IN
          SELECT * FROM ${config.postgresOutboxSchema}.${config.postgresOutboxTable} m WHERE m.id in (SELECT DISTINCT ON (segment) id
            FROM ${config.postgresOutboxSchema}.${config.postgresOutboxTable}
            WHERE processed_at IS NULL AND abandoned_at IS NULL
            ORDER BY segment, created_at) order by created_at
        LOOP
          BEGIN
            EXIT WHEN cardinality(ids) >= max_size;
          
            SELECT id, locked_until
              INTO message_row
              FROM ${config.postgresOutboxSchema}.${config.postgresOutboxTable}
              WHERE id = loop_row.id
              FOR NO KEY UPDATE NOWAIT;
            
            IF message_row.locked_until > NOW() THEN
              CONTINUE;
            END IF;
            
            ids := array_append(ids, message_row.id);
          EXCEPTION WHEN lock_not_available THEN
            CONTINUE;
          END;
        END LOOP;
        
        -- if max_size not reached: get the oldest parallelizable message independent of segment
        IF cardinality(ids) < max_size THEN
          FOR loop_row IN
            SELECT * FROM ${config.postgresOutboxSchema}.${config.postgresOutboxTable}
              WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
                AND id NOT IN (SELECT UNNEST(ids))
              order by created_at
          LOOP
            BEGIN
              EXIT WHEN cardinality(ids) >= max_size;

              SELECT *
                INTO message_row
                FROM ${config.postgresOutboxSchema}.${config.postgresOutboxTable}
                WHERE id = loop_row.id
                FOR NO KEY UPDATE NOWAIT;

              ids := array_append(ids, message_row.id);
            EXCEPTION WHEN lock_not_available THEN
              CONTINUE;
            END;
          END LOOP;
        END IF;
        
        -- set a short lock value so the the workers can each process a message
        IF cardinality(ids) > 0 THEN

          RETURN QUERY 
            UPDATE ${config.postgresOutboxSchema}.${config.postgresOutboxTable}
              SET locked_until = NOW() + INTERVAL '10 seconds', started_attempts = started_attempts + 1
              WHERE ID = ANY(ids)
              RETURNING *;

        END IF;
      END;
      $BODY$;
    `);
    logger.debug('Create indexes');
    await dbClient.query(/* sql */ `
      CREATE INDEX inbox_segment_idx ON ${config.postgresOutboxSchema}.${config.postgresOutboxTable} (segment);
      CREATE INDEX inbox_created_at_idx ON ${config.postgresOutboxSchema}.${config.postgresOutboxTable} (created_at);
      CREATE INDEX inbox_processed_at_idx ON ${config.postgresOutboxSchema}.${config.postgresOutboxTable} (processed_at);
      CREATE INDEX inbox_abandoned_at_idx ON ${config.postgresOutboxSchema}.${config.postgresOutboxTable} (abandoned_at);
    `);

    dbClient.end();
    logger.info(
      'Added the outbox table and created the publication for the outbox table',
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
      GRANT USAGE ON SEQUENCE movies_id_seq TO ${config.postgresHandlerRole};;
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
    await dbmsSetup(config);
    await outboxSetup(config);
    await testDataSetup(config);
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
})();

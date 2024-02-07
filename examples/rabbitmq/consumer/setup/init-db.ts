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

    logger.debug('Create the database inbox listener role');
    await rootClient.query(/* sql */ `
      DROP ROLE IF EXISTS ${config.postgresInboxListenerRole};
      CREATE ROLE ${config.postgresInboxListenerRole} WITH REPLICATION LOGIN PASSWORD '${config.postgresInboxListenerRolePassword}';
    `);

    logger.debug('Create the database inbox handler role');
    await rootClient.query(/* sql */ `
      DROP ROLE IF EXISTS ${config.postgresHandlerRole};
      CREATE ROLE ${config.postgresHandlerRole} WITH LOGIN PASSWORD '${config.postgresHandlerRolePassword}';
      GRANT CONNECT ON DATABASE ${config.postgresDatabase} TO ${config.postgresHandlerRole};
    `);

    rootClient.end();
    logger.info('Created the database and the inbox and handler roles');
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
    await dbClient.query(/* sql */ `
      CREATE SCHEMA IF NOT EXISTS ${config.postgresInboxSchema};
      GRANT USAGE ON SCHEMA ${config.postgresInboxSchema} TO ${config.postgresHandlerRole};
      GRANT USAGE ON SCHEMA ${config.postgresInboxSchema} TO ${config.postgresInboxListenerRole};
    `);

    logger.debug('Create the inbox table');
    await dbClient.query(/* sql */ `
      DROP TABLE IF EXISTS ${config.postgresInboxSchema}.${config.postgresInboxTable} CASCADE;
      CREATE TABLE ${config.postgresInboxSchema}.${config.postgresInboxTable} (
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
      ALTER TABLE ${config.postgresInboxSchema}.${config.postgresInboxTable} ADD CONSTRAINT inbox_concurrency_check
        CHECK (concurrency IN ('sequential', 'parallel'));
      GRANT SELECT, INSERT, DELETE ON ${config.postgresInboxSchema}.${config.postgresInboxTable} TO ${config.postgresHandlerRole};
      GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON ${config.postgresInboxSchema}.${config.postgresInboxTable} TO ${config.postgresHandlerRole};
      GRANT SELECT, INSERT, UPDATE, DELETE ON ${config.postgresInboxSchema}.${config.postgresInboxTable} TO ${config.postgresInboxListenerRole};
    `);

    logger.debug('Create the inbox publication');
    await dbClient.query(/* sql */ `
      DROP PUBLICATION IF EXISTS ${config.postgresInboxPub};
      CREATE PUBLICATION ${config.postgresInboxPub} FOR TABLE ${config.postgresInboxSchema}.${config.postgresInboxTable} WITH (publish = 'insert')
    `);
    await dbClient.query(/* sql */ `
      select pg_create_logical_replication_slot('${config.postgresInboxSlot}', 'pgoutput');
    `);

    logger.debug('Create the inbox polling function');
    await dbClient.query(/* sql */ `
      DROP FUNCTION IF EXISTS ${config.postgresInboxSchema}.${config.nextInboxMessagesFunctionName}(integer);
      CREATE OR REPLACE FUNCTION ${config.postgresInboxSchema}.${config.nextInboxMessagesFunctionName}(
        max_size integer)
          RETURNS SETOF ${config.postgresInboxSchema}.${config.postgresInboxTable} 
          LANGUAGE 'plpgsql'

      AS $BODY$
      DECLARE 
        loop_row ${config.postgresInboxSchema}.${config.postgresInboxTable}%ROWTYPE;
        message_row ${config.postgresInboxSchema}.${config.postgresInboxTable}%ROWTYPE;
        ids uuid[] := '{}';
      BEGIN

        IF max_size < 1 THEN
          RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
        END IF;

        -- get (only) the oldest message of every segment but only return it if it is not locked
        FOR loop_row IN
          SELECT * FROM ${config.postgresInboxSchema}.${config.postgresInboxTable} m WHERE m.id in (SELECT DISTINCT ON (segment) id
            FROM ${config.postgresInboxSchema}.${config.postgresInboxTable}
            WHERE processed_at IS NULL AND abandoned_at IS NULL
            ORDER BY segment, created_at) order by created_at
        LOOP
          BEGIN
            EXIT WHEN cardinality(ids) >= max_size;
          
            SELECT id, locked_until
              INTO message_row
              FROM ${config.postgresInboxSchema}.${config.postgresInboxTable}
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
            SELECT * FROM ${config.postgresInboxSchema}.${config.postgresInboxTable}
              WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
                AND id NOT IN (SELECT UNNEST(ids))
              order by created_at
          LOOP
            BEGIN
              EXIT WHEN cardinality(ids) >= max_size;

              SELECT *
                INTO message_row
                FROM ${config.postgresInboxSchema}.${config.postgresInboxTable}
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
            UPDATE ${config.postgresInboxSchema}.${config.postgresInboxTable}
              SET locked_until = NOW() + INTERVAL '10 seconds', started_attempts = started_attempts + 1
              WHERE ID = ANY(ids)
              RETURNING *;

        END IF;
      END;
      $BODY$;
    `);
    logger.debug('Create indexes');
    await dbClient.query(/* sql */ `
      CREATE INDEX inbox_segment_idx ON ${config.postgresInboxSchema}.${config.postgresInboxTable} (segment);
      CREATE INDEX inbox_created_at_idx ON ${config.postgresInboxSchema}.${config.postgresInboxTable} (created_at);
      CREATE INDEX inbox_processed_at_idx ON ${config.postgresInboxSchema}.${config.postgresInboxTable} (processed_at);
      CREATE INDEX inbox_abandoned_at_idx ON ${config.postgresInboxSchema}.${config.postgresInboxTable} (abandoned_at);
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
      GRANT USAGE ON SEQUENCE published_movies_id_seq TO ${config.postgresHandlerRole};;
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
    await dbmsSetup(config);
    await inboxSetup(config);
    await testDataSetup(config);
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
})();

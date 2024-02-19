import {
  DatabasePollingSetupConfig,
  DatabaseReplicationSetupConfig,
} from './database-setup';
import { DatabaseSetupExporter } from './database-setup-exporter';

const { createPollingScript, createReplicationScript } = DatabaseSetupExporter;

describe('Database setup exporter', () => {
  const basePollingConfig: Omit<
    DatabasePollingSetupConfig,
    'nextMessagesName' | 'outboxOrInbox' | 'table'
  > = {
    database: 'test_database',
    listenerRole: 'test_listener',
    handlerRole: 'test_handler',
    schema: 'test_schema',
    nextMessagesSchema: 'test_schema_messages',
  };

  const baseReplicationConfig: Omit<
    DatabaseReplicationSetupConfig,
    'publication' | 'replicationSlot' | 'outboxOrInbox' | 'table'
  > = {
    database: 'test_database',
    schema: 'test_schema',
    listenerRole: 'test_listener',
    handlerRole: 'test_handler',
  };

  it('should return the correct SQL for the outbox createPollingScript call', () => {
    // Arrange
    const outboxConfig: DatabasePollingSetupConfig = {
      ...basePollingConfig,
      table: 'test_outbox',
      nextMessagesName: 'test_next_outbox_messages',
      outboxOrInbox: 'outbox',
    };

    // Act
    const outboxSql = createPollingScript(outboxConfig);

    // Assert
    const expectedOutboxSql = /* sql */ `
--   ___   _   _  _____  ___   ___  __  __
--  / _ \\ | | | ||_   _|| _ ) / _ \\ \\ \\/ /
-- | (_) || |_| |  | |  | _ \\| (_) | >  < 
--  \\___/  \\___/   |_|  |___/ \\___/ /_/\\_\\
 

-- Manually create the roles if they do not exist:

-- DROP OWNED BY test_listener;
-- DROP ROLE IF EXISTS test_listener;
-- CREATE ROLE test_listener WITH LOGIN PASSWORD 'secret-password';  
-- GRANT CONNECT ON DATABASE test_database TO test_listener;  

-- DROP OWNED BY test_handler;
-- DROP ROLE IF EXISTS test_handler;
-- CREATE ROLE test_handler WITH LOGIN PASSWORD 'secret-password';
-- GRANT CONNECT ON DATABASE test_database TO test_handler;  


-- Drop and create the outbox table and ensure the schema exists

CREATE SCHEMA IF NOT EXISTS test_schema;

DROP TABLE IF EXISTS test_schema.test_outbox CASCADE;
CREATE TABLE test_schema.test_outbox (
  id uuid PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  message_type TEXT NOT NULL,
  segment TEXT,
  concurrency TEXT NOT NULL DEFAULT 'sequential',
  payload JSONB NOT NULL,
  metadata JSONB,
  locked_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  processed_at TIMESTAMPTZ,
  abandoned_at TIMESTAMPTZ,
  started_attempts smallint NOT NULL DEFAULT 0,
  finished_attempts smallint NOT NULL DEFAULT 0
);
ALTER TABLE test_schema.test_outbox ADD CONSTRAINT outbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT USAGE ON SCHEMA test_schema TO test_handler;
GRANT USAGE ON SCHEMA test_schema TO test_listener;

GRANT SELECT, INSERT, DELETE ON test_schema.test_outbox TO test_handler;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON test_schema.test_outbox TO test_handler;
GRANT SELECT, INSERT, UPDATE, DELETE ON test_schema.test_outbox TO test_listener;


-- Create the function to get the next batch of messages from the outbox table.

DROP FUNCTION IF EXISTS test_schema_messages.test_next_outbox_messages(integer, integer);
CREATE OR REPLACE FUNCTION test_schema_messages.test_next_outbox_messages(
  max_size integer, lock_ms integer)
    RETURNS SETOF test_schema.test_outbox 
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE 
  loop_row test_schema.test_outbox%ROWTYPE;
  message_row test_schema.test_outbox%ROWTYPE;
  ids uuid[] := '{}';
BEGIN

  IF max_size < 1 THEN
    RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
  END IF;

  -- get (only) the oldest message of every segment but only return it if it is not locked
  FOR loop_row IN
    SELECT * FROM test_schema.test_outbox m WHERE m.id in (SELECT DISTINCT ON (segment) id
      FROM test_schema.test_outbox
      WHERE processed_at IS NULL AND abandoned_at IS NULL
      ORDER BY segment, created_at) order by created_at
  LOOP
    BEGIN
      EXIT WHEN cardinality(ids) >= max_size;
    
      SELECT id, locked_until
        INTO message_row
        FROM test_schema.test_outbox
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
      SELECT * FROM test_schema.test_outbox
        WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
          AND id NOT IN (SELECT UNNEST(ids))
        order by created_at
    LOOP
      BEGIN
        EXIT WHEN cardinality(ids) >= max_size;

        SELECT *
          INTO message_row
          FROM test_schema.test_outbox
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
      UPDATE test_schema.test_outbox
        SET locked_until = clock_timestamp() + (lock_ms || ' milliseconds')::INTERVAL, started_attempts = started_attempts + 1
        WHERE ID = ANY(ids)
        RETURNING *;

  END IF;
END;
$BODY$;


-- Create indexes for the outbox table to improve polling performance

CREATE INDEX outbox_segment_idx ON test_schema.test_outbox (segment);
CREATE INDEX outbox_created_at_idx ON test_schema.test_outbox (created_at);
CREATE INDEX outbox_processed_at_idx ON test_schema.test_outbox (processed_at);
CREATE INDEX outbox_abandoned_at_idx ON test_schema.test_outbox (abandoned_at);

`;

    expect(outboxSql).toBe(expectedOutboxSql);
  });

  it('should return the correct SQL for the inbox createPollingScript call', () => {
    // Arrange
    const inboxConfig: DatabasePollingSetupConfig = {
      ...basePollingConfig,
      table: 'test_inbox',
      nextMessagesName: 'test_next_inbox_messages',
      outboxOrInbox: 'inbox',
    };

    // Act
    const inboxSql = createPollingScript(inboxConfig, true);

    // Assert
    const expectedInboxSql = /* sql*/ `
-- ____  _  _  ___   ___  __  __
-- |_ _|| \\| || _ ) / _ \\ \\ \\/ /
--  | | | .  || _ \\| (_) | >  < 
-- |___||_|\\_||___/ \\___/ /_/\\_\\
 


-- Drop and create the inbox table and ensure the schema exists

CREATE SCHEMA IF NOT EXISTS test_schema;

DROP TABLE IF EXISTS test_schema.test_inbox CASCADE;
CREATE TABLE test_schema.test_inbox (
  id uuid PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  message_type TEXT NOT NULL,
  segment TEXT,
  concurrency TEXT NOT NULL DEFAULT 'sequential',
  payload JSONB NOT NULL,
  metadata JSONB,
  locked_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  processed_at TIMESTAMPTZ,
  abandoned_at TIMESTAMPTZ,
  started_attempts smallint NOT NULL DEFAULT 0,
  finished_attempts smallint NOT NULL DEFAULT 0
);
ALTER TABLE test_schema.test_inbox ADD CONSTRAINT inbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT USAGE ON SCHEMA test_schema TO test_handler;
GRANT USAGE ON SCHEMA test_schema TO test_listener;

GRANT SELECT, INSERT, DELETE ON test_schema.test_inbox TO test_handler;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON test_schema.test_inbox TO test_handler;
GRANT SELECT, INSERT, UPDATE, DELETE ON test_schema.test_inbox TO test_listener;


-- Create the function to get the next batch of messages from the inbox table.

DROP FUNCTION IF EXISTS test_schema_messages.test_next_inbox_messages(integer, integer);
CREATE OR REPLACE FUNCTION test_schema_messages.test_next_inbox_messages(
  max_size integer, lock_ms integer)
    RETURNS SETOF test_schema.test_inbox 
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE 
  loop_row test_schema.test_inbox%ROWTYPE;
  message_row test_schema.test_inbox%ROWTYPE;
  ids uuid[] := '{}';
BEGIN

  IF max_size < 1 THEN
    RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
  END IF;

  -- get (only) the oldest message of every segment but only return it if it is not locked
  FOR loop_row IN
    SELECT * FROM test_schema.test_inbox m WHERE m.id in (SELECT DISTINCT ON (segment) id
      FROM test_schema.test_inbox
      WHERE processed_at IS NULL AND abandoned_at IS NULL
      ORDER BY segment, created_at) order by created_at
  LOOP
    BEGIN
      EXIT WHEN cardinality(ids) >= max_size;
    
      SELECT id, locked_until
        INTO message_row
        FROM test_schema.test_inbox
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
      SELECT * FROM test_schema.test_inbox
        WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
          AND id NOT IN (SELECT UNNEST(ids))
        order by created_at
    LOOP
      BEGIN
        EXIT WHEN cardinality(ids) >= max_size;

        SELECT *
          INTO message_row
          FROM test_schema.test_inbox
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
      UPDATE test_schema.test_inbox
        SET locked_until = clock_timestamp() + (lock_ms || ' milliseconds')::INTERVAL, started_attempts = started_attempts + 1
        WHERE ID = ANY(ids)
        RETURNING *;

  END IF;
END;
$BODY$;


-- Create indexes for the inbox table to improve polling performance

CREATE INDEX inbox_segment_idx ON test_schema.test_inbox (segment);
CREATE INDEX inbox_created_at_idx ON test_schema.test_inbox (created_at);
CREATE INDEX inbox_processed_at_idx ON test_schema.test_inbox (processed_at);
CREATE INDEX inbox_abandoned_at_idx ON test_schema.test_inbox (abandoned_at);

`;

    expect(inboxSql).toBe(expectedInboxSql);
  });

  it('should return the correct SQL for the outbox createReplicationScript call', () => {
    // Arrange
    const outboxConfig: DatabaseReplicationSetupConfig = {
      ...baseReplicationConfig,
      table: 'test_outbox',
      publication: 'test_outbox_pub',
      replicationSlot: 'test_outbox_slot',
      outboxOrInbox: 'outbox',
    };

    // Act
    const outboxSql = createReplicationScript(outboxConfig);

    // Assert
    const expectedOutboxSql = /* sql */ `
--   ___   _   _  _____  ___   ___  __  __
--  / _ \\ | | | ||_   _|| _ ) / _ \\ \\ \\/ /
-- | (_) || |_| |  | |  | _ \\| (_) | >  < 
--  \\___/  \\___/   |_|  |___/ \\___/ /_/\\_\\


-- Manually create the roles if they do not exist:

-- DROP OWNED BY test_listener;
-- DROP ROLE IF EXISTS test_listener;
-- CREATE ROLE test_listener WITH LOGIN PASSWORD 'secret-password';  
-- GRANT CONNECT ON DATABASE test_database TO test_listener;  

-- DROP OWNED BY test_handler;
-- DROP ROLE IF EXISTS test_handler;
-- CREATE ROLE test_handler WITH LOGIN PASSWORD 'secret-password';
-- GRANT CONNECT ON DATABASE test_database TO test_handler;  


-- Drop and create the outbox table and ensure the schema exists

CREATE SCHEMA IF NOT EXISTS test_schema;

DROP TABLE IF EXISTS test_schema.test_outbox CASCADE;
CREATE TABLE test_schema.test_outbox (
  id uuid PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  message_type TEXT NOT NULL,
  segment TEXT,
  concurrency TEXT NOT NULL DEFAULT 'sequential',
  payload JSONB NOT NULL,
  metadata JSONB,
  locked_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  processed_at TIMESTAMPTZ,
  abandoned_at TIMESTAMPTZ,
  started_attempts smallint NOT NULL DEFAULT 0,
  finished_attempts smallint NOT NULL DEFAULT 0
);
ALTER TABLE test_schema.test_outbox ADD CONSTRAINT outbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT USAGE ON SCHEMA test_schema TO test_handler;
GRANT USAGE ON SCHEMA test_schema TO test_listener;

GRANT SELECT, INSERT, DELETE ON test_schema.test_outbox TO test_handler;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON test_schema.test_outbox TO test_handler;
GRANT SELECT, INSERT, UPDATE, DELETE ON test_schema.test_outbox TO test_listener;


-- Assign replication role and create publication

ALTER ROLE test_listener WITH REPLICATION;
DROP PUBLICATION IF EXISTS test_outbox_pub;
CREATE PUBLICATION test_outbox_pub FOR TABLE test_schema.test_outbox WITH (publish = 'insert');


-- Create the logical replication slot

SELECT pg_drop_replication_slot('test_outbox_slot') 
  FROM pg_replication_slots WHERE slot_name = 'test_outbox_slot';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('test_outbox_slot', 'pgoutput');

`;

    expect(outboxSql).toBe(expectedOutboxSql);
  });

  it('should return the correct SQL for the inbox createReplicationScript call', () => {
    // Arrange
    const inboxConfig: DatabaseReplicationSetupConfig = {
      ...baseReplicationConfig,
      table: 'test_inbox',
      publication: 'test_inbox_pub',
      replicationSlot: 'test_inbox_slot',
      outboxOrInbox: 'inbox',
    };

    // Act
    const inboxSql = createReplicationScript(inboxConfig, true);

    // Assert
    const expectedInboxSql = /* sql*/ `
-- ____  _  _  ___   ___  __  __
-- |_ _|| \\| || _ ) / _ \\ \\ \\/ /
--  | | | .  || _ \\| (_) | >  < 
-- |___||_|\\_||___/ \\___/ /_/\\_\\



-- Drop and create the inbox table and ensure the schema exists

CREATE SCHEMA IF NOT EXISTS test_schema;

DROP TABLE IF EXISTS test_schema.test_inbox CASCADE;
CREATE TABLE test_schema.test_inbox (
  id uuid PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  message_type TEXT NOT NULL,
  segment TEXT,
  concurrency TEXT NOT NULL DEFAULT 'sequential',
  payload JSONB NOT NULL,
  metadata JSONB,
  locked_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  processed_at TIMESTAMPTZ,
  abandoned_at TIMESTAMPTZ,
  started_attempts smallint NOT NULL DEFAULT 0,
  finished_attempts smallint NOT NULL DEFAULT 0
);
ALTER TABLE test_schema.test_inbox ADD CONSTRAINT inbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT USAGE ON SCHEMA test_schema TO test_handler;
GRANT USAGE ON SCHEMA test_schema TO test_listener;

GRANT SELECT, INSERT, DELETE ON test_schema.test_inbox TO test_handler;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON test_schema.test_inbox TO test_handler;
GRANT SELECT, INSERT, UPDATE, DELETE ON test_schema.test_inbox TO test_listener;


-- Assign replication role and create publication

ALTER ROLE test_listener WITH REPLICATION;
DROP PUBLICATION IF EXISTS test_inbox_pub;
CREATE PUBLICATION test_inbox_pub FOR TABLE test_schema.test_inbox WITH (publish = 'insert');


-- Create the logical replication slot

SELECT pg_drop_replication_slot('test_inbox_slot') 
  FROM pg_replication_slots WHERE slot_name = 'test_inbox_slot';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('test_inbox_slot', 'pgoutput');

`;

    expect(inboxSql).toBe(expectedInboxSql);
  });
});

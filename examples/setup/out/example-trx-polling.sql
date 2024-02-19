
--   ___   _   _  _____  ___   ___  __  __
--  / _ \ | | | ||_   _|| _ ) / _ \ \ \/ /
-- | (_) || |_| |  | |  | _ \| (_) | >  < 
--  \___/  \___/   |_|  |___/ \___/ /_/\_\
 

-- Manually create the roles if they do not exist:

-- DROP OWNED BY messaging_listener;
-- DROP ROLE IF EXISTS messaging_listener;
-- CREATE ROLE messaging_listener WITH LOGIN PASSWORD 'secret-password';  
-- GRANT CONNECT ON DATABASE my_database TO messaging_listener;  

-- DROP OWNED BY messaging_listener;
-- DROP ROLE IF EXISTS messaging_listener;
-- CREATE ROLE messaging_listener WITH LOGIN PASSWORD 'secret-password';
-- GRANT CONNECT ON DATABASE my_database TO messaging_listener;  


-- Drop and create the outbox table and ensure the schema exists

CREATE SCHEMA IF NOT EXISTS messaging;

DROP TABLE IF EXISTS messaging.outbox CASCADE;
CREATE TABLE messaging.outbox (
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
ALTER TABLE messaging.outbox ADD CONSTRAINT outbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT USAGE ON SCHEMA messaging TO messaging_listener;
GRANT USAGE ON SCHEMA messaging TO messaging_listener;

GRANT SELECT, INSERT, DELETE ON messaging.outbox TO messaging_listener;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON messaging.outbox TO messaging_listener;
GRANT SELECT, INSERT, UPDATE, DELETE ON messaging.outbox TO messaging_listener;


-- Create the function to get the next batch of messages from the outbox table.

DROP FUNCTION IF EXISTS messaging.next_outbox_messages(integer, integer);
CREATE OR REPLACE FUNCTION messaging.next_outbox_messages(
  max_size integer, lock_ms integer)
    RETURNS SETOF messaging.outbox 
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE 
  loop_row messaging.outbox%ROWTYPE;
  message_row messaging.outbox%ROWTYPE;
  ids uuid[] := '{}';
BEGIN

  IF max_size < 1 THEN
    RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
  END IF;

  -- get (only) the oldest message of every segment but only return it if it is not locked
  FOR loop_row IN
    SELECT * FROM messaging.outbox m WHERE m.id in (SELECT DISTINCT ON (segment) id
      FROM messaging.outbox
      WHERE processed_at IS NULL AND abandoned_at IS NULL
      ORDER BY segment, created_at) order by created_at
  LOOP
    BEGIN
      EXIT WHEN cardinality(ids) >= max_size;
    
      SELECT id, locked_until
        INTO message_row
        FROM messaging.outbox
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
      SELECT * FROM messaging.outbox
        WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
          AND id NOT IN (SELECT UNNEST(ids))
        order by created_at
    LOOP
      BEGIN
        EXIT WHEN cardinality(ids) >= max_size;

        SELECT *
          INTO message_row
          FROM messaging.outbox
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
      UPDATE messaging.outbox
        SET locked_until = clock_timestamp() + (lock_ms || ' milliseconds')::INTERVAL, started_attempts = started_attempts + 1
        WHERE ID = ANY(ids)
        RETURNING *;

  END IF;
END;
$BODY$;


-- Create indexes for the outbox table to improve polling performance

CREATE INDEX outbox_segment_idx ON messaging.outbox (segment);
CREATE INDEX outbox_created_at_idx ON messaging.outbox (created_at);
CREATE INDEX outbox_processed_at_idx ON messaging.outbox (processed_at);
CREATE INDEX outbox_abandoned_at_idx ON messaging.outbox (abandoned_at);


-- ____  _  _  ___   ___  __  __
-- |_ _|| \| || _ ) / _ \ \ \/ /
--  | | | .  || _ \| (_) | >  < 
-- |___||_|\_||___/ \___/ /_/\_\
 


-- Drop and create the inbox table and ensure the schema exists

CREATE SCHEMA IF NOT EXISTS messaging;

DROP TABLE IF EXISTS messaging.inbox CASCADE;
CREATE TABLE messaging.inbox (
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
ALTER TABLE messaging.inbox ADD CONSTRAINT inbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT USAGE ON SCHEMA messaging TO messaging_listener;
GRANT USAGE ON SCHEMA messaging TO messaging_listener;

GRANT SELECT, INSERT, DELETE ON messaging.inbox TO messaging_listener;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON messaging.inbox TO messaging_listener;
GRANT SELECT, INSERT, UPDATE, DELETE ON messaging.inbox TO messaging_listener;


-- Create the function to get the next batch of messages from the inbox table.

DROP FUNCTION IF EXISTS messaging.next_inbox_messages(integer, integer);
CREATE OR REPLACE FUNCTION messaging.next_inbox_messages(
  max_size integer, lock_ms integer)
    RETURNS SETOF messaging.inbox 
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE 
  loop_row messaging.inbox%ROWTYPE;
  message_row messaging.inbox%ROWTYPE;
  ids uuid[] := '{}';
BEGIN

  IF max_size < 1 THEN
    RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
  END IF;

  -- get (only) the oldest message of every segment but only return it if it is not locked
  FOR loop_row IN
    SELECT * FROM messaging.inbox m WHERE m.id in (SELECT DISTINCT ON (segment) id
      FROM messaging.inbox
      WHERE processed_at IS NULL AND abandoned_at IS NULL
      ORDER BY segment, created_at) order by created_at
  LOOP
    BEGIN
      EXIT WHEN cardinality(ids) >= max_size;
    
      SELECT id, locked_until
        INTO message_row
        FROM messaging.inbox
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
      SELECT * FROM messaging.inbox
        WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
          AND id NOT IN (SELECT UNNEST(ids))
        order by created_at
    LOOP
      BEGIN
        EXIT WHEN cardinality(ids) >= max_size;

        SELECT *
          INTO message_row
          FROM messaging.inbox
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
      UPDATE messaging.inbox
        SET locked_until = clock_timestamp() + (lock_ms || ' milliseconds')::INTERVAL, started_attempts = started_attempts + 1
        WHERE ID = ANY(ids)
        RETURNING *;

  END IF;
END;
$BODY$;


-- Create indexes for the inbox table to improve polling performance

CREATE INDEX inbox_segment_idx ON messaging.inbox (segment);
CREATE INDEX inbox_created_at_idx ON messaging.inbox (created_at);
CREATE INDEX inbox_processed_at_idx ON messaging.inbox (processed_at);
CREATE INDEX inbox_abandoned_at_idx ON messaging.inbox (abandoned_at);


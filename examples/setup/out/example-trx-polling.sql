
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

CREATE SCHEMA IF NOT EXISTS public;

DROP TABLE IF EXISTS public.outbox CASCADE;
CREATE TABLE public.outbox (
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
ALTER TABLE public.outbox ADD CONSTRAINT outbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT USAGE ON SCHEMA public TO messaging_listener;
GRANT USAGE ON SCHEMA public TO messaging_listener;

GRANT SELECT, INSERT, DELETE ON public.outbox TO messaging_listener;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON public.outbox TO messaging_listener;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.outbox TO messaging_listener;


-- Create the function to get the next batch of messages from the outbox table.

DROP FUNCTION IF EXISTS public.next_outbox_messages(integer, integer);
CREATE OR REPLACE FUNCTION public.next_outbox_messages(
  max_size integer, lock_ms integer)
    RETURNS SETOF public.outbox 
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE 
  loop_row public.outbox%ROWTYPE;
  message_row public.outbox%ROWTYPE;
  ids uuid[] := '{}';
BEGIN

  IF max_size < 1 THEN
    RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
  END IF;

  -- get (only) the oldest message of every segment but only return it if it is not locked
  FOR loop_row IN
    SELECT * FROM public.outbox m WHERE m.id in (SELECT DISTINCT ON (segment) id
      FROM public.outbox
      WHERE processed_at IS NULL AND abandoned_at IS NULL
      ORDER BY segment, created_at) order by created_at
  LOOP
    BEGIN
      EXIT WHEN cardinality(ids) >= max_size;

      -- if the message is marked as locked --> skip it
      IF loop_row.locked_until > NOW() THEN
        CONTINUE;
      END IF;
    
      SELECT *
        INTO message_row
        FROM public.outbox
        WHERE id = loop_row.id
        FOR NO KEY UPDATE NOWAIT; -- throw/catch error when locked
      
      ids := array_append(ids, message_row.id);
    EXCEPTION 
      WHEN lock_not_available THEN
        CONTINUE;
      WHEN serialization_failure THEN
        CONTINUE;
    END;
  END LOOP;
  
  -- if max_size not reached: get the oldest parallelizable message independent of segment
  IF cardinality(ids) < max_size THEN
    FOR loop_row IN
      SELECT * FROM public.outbox
        WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
          AND id NOT IN (SELECT UNNEST(ids))
        order by created_at
    LOOP
      BEGIN
        EXIT WHEN cardinality(ids) >= max_size;

        SELECT *
          INTO message_row
          FROM public.outbox
          WHERE id = loop_row.id
          FOR NO KEY UPDATE NOWAIT; -- throw/catch error when locked

        ids := array_append(ids, message_row.id);
      EXCEPTION 
        WHEN lock_not_available THEN
          CONTINUE;
        WHEN serialization_failure THEN
          CONTINUE;
      END;
    END LOOP;
  END IF;
  
  -- set a short lock value so the the workers can each process a message
  IF cardinality(ids) > 0 THEN

    RETURN QUERY 
      UPDATE public.outbox
        SET locked_until = clock_timestamp() + (lock_ms || ' milliseconds')::INTERVAL, started_attempts = started_attempts + 1
        WHERE ID = ANY(ids)
        RETURNING *;

  END IF;
END;
$BODY$;


-- Create indexes for the outbox table to improve polling performance

DROP INDEX IF EXISTS outbox_segment_idx;
CREATE INDEX outbox_segment_idx ON public.outbox (segment);
DROP INDEX IF EXISTS outbox_created_at_idx;
CREATE INDEX outbox_created_at_idx ON public.outbox (created_at);
DROP INDEX IF EXISTS outbox_processed_at_idx;
CREATE INDEX outbox_processed_at_idx ON public.outbox (processed_at);
DROP INDEX IF EXISTS outbox_abandoned_at_idx;
CREATE INDEX outbox_abandoned_at_idx ON public.outbox (abandoned_at);
DROP INDEX IF EXISTS outbox_locked_until_idx;
CREATE INDEX outbox_locked_until_idx ON public.outbox (locked_until);


-- ____  _  _  ___   ___  __  __
-- |_ _|| \| || _ ) / _ \ \ \/ /
--  | | | .  || _ \| (_) | >  < 
-- |___||_|\_||___/ \___/ /_/\_\
 


-- Drop and create the inbox table and ensure the schema exists

CREATE SCHEMA IF NOT EXISTS public;

DROP TABLE IF EXISTS public.inbox CASCADE;
CREATE TABLE public.inbox (
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
ALTER TABLE public.inbox ADD CONSTRAINT inbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT USAGE ON SCHEMA public TO messaging_listener;
GRANT USAGE ON SCHEMA public TO messaging_listener;

GRANT SELECT, INSERT, DELETE ON public.inbox TO messaging_listener;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON public.inbox TO messaging_listener;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.inbox TO messaging_listener;


-- Create the function to get the next batch of messages from the inbox table.

DROP FUNCTION IF EXISTS public.next_inbox_messages(integer, integer);
CREATE OR REPLACE FUNCTION public.next_inbox_messages(
  max_size integer, lock_ms integer)
    RETURNS SETOF public.inbox 
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE 
  loop_row public.inbox%ROWTYPE;
  message_row public.inbox%ROWTYPE;
  ids uuid[] := '{}';
BEGIN

  IF max_size < 1 THEN
    RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
  END IF;

  -- get (only) the oldest message of every segment but only return it if it is not locked
  FOR loop_row IN
    SELECT * FROM public.inbox m WHERE m.id in (SELECT DISTINCT ON (segment) id
      FROM public.inbox
      WHERE processed_at IS NULL AND abandoned_at IS NULL
      ORDER BY segment, created_at) order by created_at
  LOOP
    BEGIN
      EXIT WHEN cardinality(ids) >= max_size;

      -- if the message is marked as locked --> skip it
      IF loop_row.locked_until > NOW() THEN
        CONTINUE;
      END IF;
    
      SELECT *
        INTO message_row
        FROM public.inbox
        WHERE id = loop_row.id
        FOR NO KEY UPDATE NOWAIT; -- throw/catch error when locked
      
      ids := array_append(ids, message_row.id);
    EXCEPTION 
      WHEN lock_not_available THEN
        CONTINUE;
      WHEN serialization_failure THEN
        CONTINUE;
    END;
  END LOOP;
  
  -- if max_size not reached: get the oldest parallelizable message independent of segment
  IF cardinality(ids) < max_size THEN
    FOR loop_row IN
      SELECT * FROM public.inbox
        WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
          AND id NOT IN (SELECT UNNEST(ids))
        order by created_at
    LOOP
      BEGIN
        EXIT WHEN cardinality(ids) >= max_size;

        SELECT *
          INTO message_row
          FROM public.inbox
          WHERE id = loop_row.id
          FOR NO KEY UPDATE NOWAIT; -- throw/catch error when locked

        ids := array_append(ids, message_row.id);
      EXCEPTION 
        WHEN lock_not_available THEN
          CONTINUE;
        WHEN serialization_failure THEN
          CONTINUE;
      END;
    END LOOP;
  END IF;
  
  -- set a short lock value so the the workers can each process a message
  IF cardinality(ids) > 0 THEN

    RETURN QUERY 
      UPDATE public.inbox
        SET locked_until = clock_timestamp() + (lock_ms || ' milliseconds')::INTERVAL, started_attempts = started_attempts + 1
        WHERE ID = ANY(ids)
        RETURNING *;

  END IF;
END;
$BODY$;


-- Create indexes for the inbox table to improve polling performance

DROP INDEX IF EXISTS inbox_segment_idx;
CREATE INDEX inbox_segment_idx ON public.inbox (segment);
DROP INDEX IF EXISTS inbox_created_at_idx;
CREATE INDEX inbox_created_at_idx ON public.inbox (created_at);
DROP INDEX IF EXISTS inbox_processed_at_idx;
CREATE INDEX inbox_processed_at_idx ON public.inbox (processed_at);
DROP INDEX IF EXISTS inbox_abandoned_at_idx;
CREATE INDEX inbox_abandoned_at_idx ON public.inbox (abandoned_at);
DROP INDEX IF EXISTS inbox_locked_until_idx;
CREATE INDEX inbox_locked_until_idx ON public.inbox (locked_until);


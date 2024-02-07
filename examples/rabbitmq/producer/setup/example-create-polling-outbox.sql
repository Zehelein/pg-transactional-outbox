-- Create script for the inbox database with polling

-- Drop and create the handler and listener roles

DROP OWNED BY db_outbox_listener;
DROP ROLE IF EXISTS db_outbox_listener;
CREATE ROLE db_outbox_listener WITH LOGIN PASSWORD 'db_outbox_listener_password';  
GRANT CONNECT ON DATABASE pg_transactional_outbox TO db_outbox_listener;  

DROP OWNED BY db_outbox_handler;
DROP ROLE IF EXISTS db_outbox_handler;
CREATE ROLE db_outbox_handler WITH LOGIN PASSWORD 'db_outbox_handler_password';
GRANT CONNECT ON DATABASE pg_transactional_outbox TO db_outbox_handler;  


-- Drop and create the inbox table and ensure the schema exists

CREATE SCHEMA IF NOT EXISTS outbox;

DROP TABLE IF EXISTS outbox.outbox CASCADE;
CREATE TABLE outbox.outbox (
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
ALTER TABLE outbox.outbox ADD CONSTRAINT outbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT USAGE ON SCHEMA outbox TO db_outbox_handler;
GRANT USAGE ON SCHEMA outbox TO db_outbox_listener;

GRANT SELECT, INSERT, DELETE ON outbox.outbox TO db_outbox_handler;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON outbox.outbox TO db_outbox_handler;
GRANT SELECT, INSERT, UPDATE, DELETE ON outbox.outbox TO db_outbox_listener;


-- Create the function to get the next batch of messages from the outbox or inbox table.

DROP FUNCTION IF EXISTS outbox.next_outbox_messages(integer, integer);
CREATE OR REPLACE FUNCTION outbox.next_outbox_messages(
  max_size integer, lock_ms integer)
    RETURNS SETOF outbox.outbox 
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE 
  loop_row outbox.outbox%ROWTYPE;
  message_row outbox.outbox%ROWTYPE;
  ids uuid[] := '{}';
BEGIN

  IF max_size < 1 THEN
    RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
  END IF;

  -- get (only) the oldest message of every segment but only return it if it is not locked
  FOR loop_row IN
    SELECT * FROM outbox.outbox m WHERE m.id in (SELECT DISTINCT ON (segment) id
      FROM outbox.outbox
      WHERE processed_at IS NULL AND abandoned_at IS NULL
      ORDER BY segment, created_at) order by created_at
  LOOP
    BEGIN
      EXIT WHEN cardinality(ids) >= max_size;
    
      SELECT id, locked_until
        INTO message_row
        FROM outbox.outbox
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
      SELECT * FROM outbox.outbox
        WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
          AND id NOT IN (SELECT UNNEST(ids))
        order by created_at
    LOOP
      BEGIN
        EXIT WHEN cardinality(ids) >= max_size;

        SELECT *
          INTO message_row
          FROM outbox.outbox
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
      UPDATE outbox.outbox
        SET locked_until = NOW() + (lock_ms || ' milliseconds')::INTERVAL, started_attempts = started_attempts + 1
        WHERE ID = ANY(ids)
        RETURNING *;

  END IF;
END;
$BODY$;


-- Create indexes for the inbox table to improve polling performance

CREATE INDEX outbox_segment_idx ON outbox.outbox (segment);
CREATE INDEX outbox_created_at_idx ON outbox.outbox (created_at);
CREATE INDEX outbox_processed_at_idx ON outbox.outbox (processed_at);
CREATE INDEX outbox_abandoned_at_idx ON outbox.outbox (abandoned_at);


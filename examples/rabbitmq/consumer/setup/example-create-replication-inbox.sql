-- Create script for the inbox database with replication

-- Drop and create the handler and listener roles

DROP OWNED BY db_inbox_listener;
DROP ROLE IF EXISTS db_inbox_listener;
CREATE ROLE db_inbox_listener WITH LOGIN PASSWORD 'db_inbox_listener_password';  
GRANT CONNECT ON DATABASE pg_transactional_inbox TO db_inbox_listener;  

DROP OWNED BY db_inbox_handler;
DROP ROLE IF EXISTS db_inbox_handler;
CREATE ROLE db_inbox_handler WITH LOGIN PASSWORD 'db_inbox_handler_password';
GRANT CONNECT ON DATABASE pg_transactional_inbox TO db_inbox_handler;  


-- Drop and create the inbox table and ensure the schema exists

CREATE SCHEMA IF NOT EXISTS inbox;

DROP TABLE IF EXISTS inbox.inbox CASCADE;
CREATE TABLE inbox.inbox (
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
ALTER TABLE inbox.inbox ADD CONSTRAINT inbox_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));

-- Grant permissions for the handler and listener role 

GRANT SELECT, INSERT, DELETE ON inbox.inbox TO db_inbox_handler;
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON inbox.inbox TO db_inbox_handler;
GRANT SELECT, INSERT, UPDATE, DELETE ON inbox.inbox TO db_inbox_listener;

GRANT USAGE ON SCHEMA inbox TO db_inbox_handler;
GRANT USAGE ON SCHEMA inbox TO db_inbox_listener;


-- Assign replication role and create publication

ALTER ROLE db_inbox_listener WITH REPLICATION;
DROP PUBLICATION IF EXISTS pg_transactional_inbox_pub;
CREATE PUBLICATION pg_transactional_inbox_pub FOR TABLE inbox.inbox WITH (publish = 'insert');


-- Create the logical replication slot

SELECT pg_drop_replication_slot('pg_transactional_inbox_slot') 
  FROM pg_replication_slots WHERE slot_name = 'pg_transactional_inbox_slot';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('pg_transactional_inbox_slot', 'pgoutput');


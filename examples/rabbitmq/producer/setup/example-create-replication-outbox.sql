-- Create script for the inbox database with replication

-- Manually create the roles if they do not exist:

-- DROP OWNED BY db_outbox_listener;
-- DROP ROLE IF EXISTS db_outbox_listener;
-- CREATE ROLE db_outbox_listener WITH LOGIN PASSWORD 'secret-password';  
-- GRANT CONNECT ON DATABASE pg_transactional_outbox TO db_outbox_listener;  

-- DROP OWNED BY db_outbox_handler;
-- DROP ROLE IF EXISTS db_outbox_handler;
-- CREATE ROLE db_outbox_handler WITH LOGIN PASSWORD 'secret-password';
-- GRANT CONNECT ON DATABASE pg_transactional_outbox TO db_outbox_handler;  


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


-- Assign replication role and create publication

ALTER ROLE db_outbox_listener WITH REPLICATION;
DROP PUBLICATION IF EXISTS pg_transactional_outbox_pub;
CREATE PUBLICATION pg_transactional_outbox_pub FOR TABLE outbox.outbox WITH (publish = 'insert');


-- Create the logical replication slot

SELECT pg_drop_replication_slot('pg_transactional_outbox_slot') 
  FROM pg_replication_slots WHERE slot_name = 'pg_transactional_outbox_slot';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('pg_transactional_outbox_slot', 'pgoutput');



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


-- Assign replication role and create publication

ALTER ROLE messaging_listener WITH REPLICATION;
DROP PUBLICATION IF EXISTS transactional_outbox_publication;
CREATE PUBLICATION transactional_outbox_publication FOR TABLE messaging.outbox WITH (publish = 'insert');


-- Create the logical replication slot

SELECT pg_drop_replication_slot('transactional_outbox_slot') 
  FROM pg_replication_slots WHERE slot_name = 'transactional_outbox_slot';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('transactional_outbox_slot', 'pgoutput');


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


-- Assign replication role and create publication

ALTER ROLE messaging_listener WITH REPLICATION;
DROP PUBLICATION IF EXISTS transactional_inbox_publication;
CREATE PUBLICATION transactional_inbox_publication FOR TABLE messaging.inbox WITH (publish = 'insert');


-- Create the logical replication slot

SELECT pg_drop_replication_slot('transactional_inbox_slot') 
  FROM pg_replication_slots WHERE slot_name = 'transactional_inbox_slot';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('transactional_inbox_slot', 'pgoutput');


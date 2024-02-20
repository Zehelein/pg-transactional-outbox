
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


-- Assign replication role and create publication

ALTER ROLE messaging_listener WITH REPLICATION;
DROP PUBLICATION IF EXISTS pg_transactional_outbox_pub;
CREATE PUBLICATION pg_transactional_outbox_pub FOR TABLE public.outbox WITH (publish = 'insert');


-- Create the logical replication slot

SELECT pg_drop_replication_slot('pg_transactional_outbox_slot') 
  FROM pg_replication_slots WHERE slot_name = 'pg_transactional_outbox_slot';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('pg_transactional_outbox_slot', 'pgoutput');


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


-- Assign replication role and create publication

ALTER ROLE messaging_listener WITH REPLICATION;
DROP PUBLICATION IF EXISTS pg_transactional_inbox_pub;
CREATE PUBLICATION pg_transactional_inbox_pub FOR TABLE public.inbox WITH (publish = 'insert');


-- Create the logical replication slot

SELECT pg_drop_replication_slot('pg_transactional_inbox_slot') 
  FROM pg_replication_slots WHERE slot_name = 'pg_transactional_inbox_slot';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('pg_transactional_inbox_slot', 'pgoutput');


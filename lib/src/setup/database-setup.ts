import { OutboxOrInbox } from '../common/base-config';

export interface DatabaseSetupConfig {
  outboxOrInbox: OutboxOrInbox;
  database: string;
  schema: string;
  table: string;
  listenerRole: string;
  handlerRole?: string;
}

export interface DatabaseReplicationSetupConfig extends DatabaseSetupConfig {
  publication: string;
  replicationSlot: string;
}

export interface DatabasePollingSetupConfig extends DatabaseSetupConfig {
  nextMessagesSchema?: string;
  nextMessagesName: string;
}

/** Create the database roles and grant connect permissions */
const dropAndCreateHandlerAndListenerRoles = ({
  database,
  listenerRole,
  handlerRole,
}: DatabaseSetupConfig): string => {
  let sql = /* sql */ `
-- DROP OWNED BY ${listenerRole};
-- DROP ROLE IF EXISTS ${listenerRole};
-- CREATE ROLE ${listenerRole} WITH LOGIN PASSWORD 'secret-password';  
-- GRANT CONNECT ON DATABASE ${database} TO ${listenerRole};  
`;

  if (handlerRole) {
    sql += /* sql */ `
-- DROP OWNED BY ${handlerRole};
-- DROP ROLE IF EXISTS ${handlerRole};
-- CREATE ROLE ${handlerRole} WITH LOGIN PASSWORD 'secret-password';
-- GRANT CONNECT ON DATABASE ${database} TO ${handlerRole};  
`;
  }
  return sql;
};

/**
 * Create the database table for the outbox or inbox. It ensures that the
 * schema of th database exists as well.
 */
const dropAndCreateTable = ({
  outboxOrInbox,
  schema,
  table,
}: DatabaseSetupConfig): string => {
  return /* sql */ `
CREATE SCHEMA IF NOT EXISTS ${schema};

DROP TABLE IF EXISTS ${schema}.${table} CASCADE;
CREATE TABLE ${schema}.${table} (
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
ALTER TABLE ${schema}.${table} ADD CONSTRAINT ${outboxOrInbox}_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));`;
};

/** Grant permissions for the handler and listener role */
const grantPermissions = ({
  handlerRole,
  listenerRole,
  schema,
  table,
}: DatabaseSetupConfig): string => {
  const hndUser = handlerRole ?? listenerRole;
  return /* sql */ `
GRANT USAGE ON SCHEMA ${schema} TO ${hndUser};
GRANT USAGE ON SCHEMA ${schema} TO ${listenerRole};

GRANT SELECT, INSERT, DELETE ON ${schema}.${table} TO ${hndUser};
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON ${schema}.${table} TO ${hndUser};
GRANT SELECT, INSERT, UPDATE, DELETE ON ${schema}.${table} TO ${listenerRole};
`;
};

/** Add the replication role to the listener role and creates the publication */
const setupReplicationCore = ({
  listenerRole,
  publication,
  schema,
  table,
}: DatabaseReplicationSetupConfig): string => {
  return /* sql */ `
ALTER ROLE ${listenerRole} WITH REPLICATION;
DROP PUBLICATION IF EXISTS ${publication};
CREATE PUBLICATION ${publication} FOR TABLE ${schema}.${table} WITH (publish = 'insert');
`;
};

/** After running the setupReplicationCore run this to create the slot */
const setupReplicationSlot = ({
  replicationSlot,
}: DatabaseReplicationSetupConfig): string => {
  return /* sql */ `
SELECT pg_drop_replication_slot('${replicationSlot}') 
  FROM pg_replication_slots WHERE slot_name = '${replicationSlot}';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('${replicationSlot}', 'pgoutput');
`;
};

/**
 * Create the function to get the next batch of messages from the outbox or
 * inbox table.
 */
const createPollingFunction = ({
  schema,
  table,
  nextMessagesSchema,
  nextMessagesName,
}: DatabasePollingSetupConfig): string => {
  const funcSchema = nextMessagesSchema ?? schema;
  return /* sql */ `
DROP FUNCTION IF EXISTS ${funcSchema}.${nextMessagesName}(integer, integer);
CREATE OR REPLACE FUNCTION ${funcSchema}.${nextMessagesName}(
  max_size integer, lock_ms integer)
    RETURNS SETOF ${schema}.${table} 
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE 
  loop_row ${schema}.${table}%ROWTYPE;
  message_row ${schema}.${table}%ROWTYPE;
  ids uuid[] := '{}';
BEGIN

  IF max_size < 1 THEN
    RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
  END IF;

  -- get (only) the oldest message of every segment but only return it if it is not locked
  FOR loop_row IN
    SELECT * FROM ${schema}.${table} m WHERE m.id in (SELECT DISTINCT ON (segment) id
      FROM ${schema}.${table}
      WHERE processed_at IS NULL AND abandoned_at IS NULL
      ORDER BY segment, created_at) order by created_at
  LOOP
    BEGIN
      EXIT WHEN cardinality(ids) >= max_size;
    
      SELECT id, locked_until
        INTO message_row
        FROM ${schema}.${table}
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
      SELECT * FROM ${schema}.${table}
        WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
          AND id NOT IN (SELECT UNNEST(ids))
        order by created_at
    LOOP
      BEGIN
        EXIT WHEN cardinality(ids) >= max_size;

        SELECT *
          INTO message_row
          FROM ${schema}.${table}
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
      UPDATE ${schema}.${table}
        SET locked_until = NOW() + (lock_ms || ' milliseconds')::INTERVAL, started_attempts = started_attempts + 1
        WHERE ID = ANY(ids)
        RETURNING *;

  END IF;
END;
$BODY$;
`;
};

/** After running the setupReplicationCore run this to create the slot */
const setupPollingIndexes = ({
  schema,
  table,
  outboxOrInbox,
}: DatabasePollingSetupConfig): string => {
  return /* sql */ `
CREATE INDEX ${outboxOrInbox}_segment_idx ON ${schema}.${table} (segment);
CREATE INDEX ${outboxOrInbox}_created_at_idx ON ${schema}.${table} (created_at);
CREATE INDEX ${outboxOrInbox}_processed_at_idx ON ${schema}.${table} (processed_at);
CREATE INDEX ${outboxOrInbox}_abandoned_at_idx ON ${schema}.${table} (abandoned_at);
`;
};

export const DatabaseSetup = {
  dropAndCreateTable,
  dropAndCreateHandlerAndListenerRoles,
  grantPermissions,
  setupReplicationCore,
  setupReplicationSlot,
  createPollingFunction,
  setupPollingIndexes,
};

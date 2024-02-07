import { ListenerConfig } from '../common/base-config';
import { PollingConfig } from '../polling/config';
import { ReplicationConfig } from '../replication/config';

/** Create the database roles and grant connect permissions */
const dropAndCreateHandlerAndListenerRoles = ({
  dbListenerConfig,
  dbHandlerConfig,
}: ListenerConfig): string => {
  let sql = /* sql */ `
DROP OWNED BY ${dbListenerConfig.user};
DROP ROLE IF EXISTS ${dbListenerConfig.user};
CREATE ROLE ${dbListenerConfig.user} WITH LOGIN PASSWORD '${dbListenerConfig.password}';  
GRANT CONNECT ON DATABASE ${dbListenerConfig.database} TO ${dbListenerConfig.user};  
`;

  if (dbHandlerConfig) {
    sql += /* sql */ `
DROP OWNED BY ${dbHandlerConfig.user};
DROP ROLE IF EXISTS ${dbHandlerConfig.user};
CREATE ROLE ${dbHandlerConfig.user} WITH LOGIN PASSWORD '${dbHandlerConfig.password}';
GRANT CONNECT ON DATABASE ${dbHandlerConfig.database} TO ${dbHandlerConfig.user};  
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
  settings,
}: ListenerConfig): string => {
  return /* sql */ `
CREATE SCHEMA IF NOT EXISTS ${settings.dbSchema};

DROP TABLE IF EXISTS ${settings.dbSchema}.${settings.dbTable} CASCADE;
CREATE TABLE ${settings.dbSchema}.${settings.dbTable} (
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
ALTER TABLE ${settings.dbSchema}.${settings.dbTable} ADD CONSTRAINT ${outboxOrInbox}_concurrency_check
  CHECK (concurrency IN ('sequential', 'parallel'));`;
};

/** Grant permissions for the handler and listener role */
const grantPermissions = ({
  dbListenerConfig,
  dbHandlerConfig,
  settings,
}: ListenerConfig): string => {
  dbHandlerConfig = dbHandlerConfig ?? dbListenerConfig;
  return /* sql */ `
GRANT USAGE ON SCHEMA ${settings.dbSchema} TO ${dbHandlerConfig.user};
GRANT USAGE ON SCHEMA ${settings.dbSchema} TO ${dbListenerConfig.user};

GRANT SELECT, INSERT, DELETE ON ${settings.dbSchema}.${settings.dbTable} TO ${dbHandlerConfig.user};
GRANT UPDATE (locked_until, processed_at, abandoned_at, started_attempts, finished_attempts) ON ${settings.dbSchema}.${settings.dbTable} TO ${dbHandlerConfig.user};
GRANT SELECT, INSERT, UPDATE, DELETE ON ${settings.dbSchema}.${settings.dbTable} TO ${dbListenerConfig.user};
`;
};

/** Add the replication role to the listener role and creates the publication */
const setupReplicationCore = ({
  dbListenerConfig,
  settings,
}: ReplicationConfig): string => {
  return /* sql */ `
ALTER ROLE ${dbListenerConfig.user} WITH REPLICATION;
DROP PUBLICATION IF EXISTS ${settings.postgresPub};
CREATE PUBLICATION ${settings.postgresPub} FOR TABLE ${settings.dbSchema}.${settings.dbTable} WITH (publish = 'insert');
`;
};

/** After running the setupReplicationCore run this to create the slot */
const setupReplicationSlot = ({ settings }: ReplicationConfig): string => {
  return /* sql */ `
SELECT pg_drop_replication_slot('${settings.postgresSlot}') 
  FROM pg_replication_slots WHERE slot_name = '${settings.postgresSlot}';

-- NOTE: This must be run in a separate database transaction or it will fail
SELECT pg_create_logical_replication_slot('${settings.postgresSlot}', 'pgoutput');
`;
};

/**
 * Create the function to get the next batch of messages from the outbox or
 * inbox table.
 */
const createPollingFunction = ({ settings }: PollingConfig): string => {
  const schema = settings.nextMessagesFunctionSchema ?? settings.dbSchema;
  return /* sql */ `
DROP FUNCTION IF EXISTS ${schema}.${settings.nextMessagesFunctionName}(integer);
CREATE OR REPLACE FUNCTION ${schema}.${settings.nextMessagesFunctionName}(
  max_size integer)
    RETURNS SETOF ${settings.dbSchema}.${settings.dbTable} 
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE 
  loop_row ${settings.dbSchema}.${settings.dbTable}%ROWTYPE;
  message_row ${settings.dbSchema}.${settings.dbTable}%ROWTYPE;
  ids uuid[] := '{}';
BEGIN

  IF max_size < 1 THEN
    RAISE EXCEPTION 'The max_size for the next messages batch must be at least one.' using errcode = 'MAXNR';
  END IF;

  -- get (only) the oldest message of every segment but only return it if it is not locked
  FOR loop_row IN
    SELECT * FROM ${settings.dbSchema}.${settings.dbTable} m WHERE m.id in (SELECT DISTINCT ON (segment) id
      FROM ${settings.dbSchema}.${settings.dbTable}
      WHERE processed_at IS NULL AND abandoned_at IS NULL
      ORDER BY segment, created_at) order by created_at
  LOOP
    BEGIN
      EXIT WHEN cardinality(ids) >= max_size;
    
      SELECT id, locked_until
        INTO message_row
        FROM ${settings.dbSchema}.${settings.dbTable}
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
      SELECT * FROM ${settings.dbSchema}.${settings.dbTable}
        WHERE concurrency = 'parallel' AND processed_at IS NULL AND abandoned_at IS NULL AND locked_until < NOW() 
          AND id NOT IN (SELECT UNNEST(ids))
        order by created_at
    LOOP
      BEGIN
        EXIT WHEN cardinality(ids) >= max_size;

        SELECT *
          INTO message_row
          FROM ${settings.dbSchema}.${settings.dbTable}
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
      UPDATE ${settings.dbSchema}.${settings.dbTable}
        SET locked_until = NOW() + INTERVAL '10 seconds', started_attempts = started_attempts + 1
        WHERE ID = ANY(ids)
        RETURNING *;

  END IF;
END;
$BODY$;
`;
};

/** After running the setupReplicationCore run this to create the slot */
const setupPollingIndexes = ({
  settings,
  outboxOrInbox,
}: ListenerConfig): string => {
  return /* sql */ `
CREATE INDEX ${outboxOrInbox}_segment_idx ON ${settings.dbSchema}.${settings.dbTable} (segment);
CREATE INDEX ${outboxOrInbox}_created_at_idx ON ${settings.dbSchema}.${settings.dbTable} (created_at);
CREATE INDEX ${outboxOrInbox}_processed_at_idx ON ${settings.dbSchema}.${settings.dbTable} (processed_at);
CREATE INDEX ${outboxOrInbox}_abandoned_at_idx ON ${settings.dbSchema}.${settings.dbTable} (abandoned_at);
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

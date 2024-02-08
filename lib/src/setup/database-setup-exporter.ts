import {
  DatabasePollingSetupConfig,
  DatabaseReplicationSetupConfig,
  DatabaseSetup,
} from './database-setup';
const {
  dropAndCreateHandlerAndListenerRoles,
  dropAndCreateTable,
  grantPermissions,
  setupReplicationCore,
  setupReplicationSlot,
  createPollingFunction,
  setupPollingIndexes,
} = DatabaseSetup;

const createReplicationScript = (
  config: DatabaseReplicationSetupConfig,
): string => {
  return `-- Create script for the inbox database with replication

-- Manually create the roles if they do not exist:
${dropAndCreateHandlerAndListenerRoles(config)}

-- Drop and create the inbox table and ensure the schema exists
${dropAndCreateTable(config)}

-- Grant permissions for the handler and listener role 
${grantPermissions(config)}

-- Assign replication role and create publication
${setupReplicationCore(config)}

-- Create the logical replication slot
${setupReplicationSlot(config)}
`;
};

const createPollingScript = (config: DatabasePollingSetupConfig): string => {
  return `-- Create script for the inbox database with polling

-- Manually create the roles if they do not exist:
${dropAndCreateHandlerAndListenerRoles(config)}

-- Drop and create the inbox table and ensure the schema exists
${dropAndCreateTable(config)}

-- Grant permissions for the handler and listener role 
${grantPermissions(config)}

-- Create the function to get the next batch of messages from the outbox or inbox table.
${createPollingFunction(config)}

-- Create indexes for the inbox table to improve polling performance
${setupPollingIndexes(config)}
`;
};

export const DatabaseSetupExporter = {
  createReplicationScript,
  createPollingScript,
};

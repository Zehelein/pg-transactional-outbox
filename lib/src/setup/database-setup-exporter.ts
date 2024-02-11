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

const inboxAsciiArt = `
-- ____  _  _  ___   ___  __  __
-- |_ _|| \\| || _ ) / _ \\ \\ \\/ /
--  | | | .  || _ \\| (_) | >  < 
-- |___||_|\\_||___/ \\___/ /_/\\_\\
`;

const outboxAsciiArt = `
--   ___   _   _  _____  ___   ___  __  __
--  / _ \\ | | | ||_   _|| _ ) / _ \\ \\ \\/ /
-- | (_) || |_| |  | |  | _ \\| (_) | >  < 
--  \\___/  \\___/   |_|  |___/ \\___/ /_/\\_\\
`;

const createReplicationScript = (
  config: DatabaseReplicationSetupConfig,
  skipRoles = false,
): string => {
  const box = config.outboxOrInbox;
  return `${box === 'inbox' ? inboxAsciiArt : outboxAsciiArt}
${skipRoles ? '' : dropAndCreateHandlerAndListenerRoles(config)}

-- Drop and create the ${box} table and ensure the schema exists
${dropAndCreateTable(config)}

-- Grant permissions for the handler and listener role 
${grantPermissions(config)}

-- Assign replication role and create publication
${setupReplicationCore(config)}

-- Create the logical replication slot
${setupReplicationSlot(config)}
`;
};

const createPollingScript = (
  config: DatabasePollingSetupConfig,
  skipRoles = false,
): string => {
  const box = config.outboxOrInbox;
  return `${box === 'inbox' ? inboxAsciiArt : outboxAsciiArt} 
${skipRoles ? '' : dropAndCreateHandlerAndListenerRoles(config)}

-- Drop and create the ${box} table and ensure the schema exists
${dropAndCreateTable(config)}

-- Grant permissions for the handler and listener role 
${grantPermissions(config)}

-- Create the function to get the next batch of messages from the ${box} table.
${createPollingFunction(config)}

-- Create indexes for the ${box} table to improve polling performance
${setupPollingIndexes(config)}
`;
};

export const DatabaseSetupExporter = {
  createReplicationScript,
  createPollingScript,
};

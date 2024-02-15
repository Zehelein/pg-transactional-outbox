import {
  BooleanSetting,
  Env,
  NumberSetting,
  StringSetting,
  getConfigSettings,
  getEnvVariableNumber,
  getEnvVariableString,
  printConfigSettings,
} from '../common/env-settings';
import {
  ListenerConfig,
  ListenerSettings,
  fallbackEnvPrefix,
  getInboxListenerSettings,
  getOutboxListenerSettings,
  inboxEnvPrefix,
  outboxEnvPrefix,
  printInboxListenerEnvVariables,
  printOutboxListenerEnvVariables,
} from '../common/listener-config';

export interface ReplicationListenerConfig extends ListenerConfig {
  /** Replication listener specific configurations */
  settings: ReplicationListenerSettings;
}

export interface ReplicationListenerSettings extends ListenerSettings {
  /** The name of the used PostgreSQL publication */
  dbPublication: string;
  /** The name of the used PostgreSQL logical replication slot */
  dbReplicationSlot: string;
  /** When there is a message processing error it restarts the logical replication subscription with a delay. This setting defines this delay in milliseconds. Default is 250ms. */
  restartDelayInMs?: number;
  /** When the replication slot is in use e.g. by another service, this service will still continue to try to connect in case the other service stops. Delay is given in milliseconds, the default is 10s. */
  restartDelaySlotInUseInMs?: number;
}

const basicSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'RESTART_DELAY_IN_MS',
    default: 250,
    func: getEnvVariableNumber,
  },
  {
    constantName: 'RESTART_DELAY_SLOT_IN_USE_IN_MS',
    default: 10000,
    func: getEnvVariableNumber,
  },
];

const inboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'DB_PUBLICATION',
    default: 'pg_transactional_inbox_pub',
    func: getEnvVariableString,
    skipFallback: true,
  },
  {
    constantName: 'DB_REPLICATION_SLOT',
    default: 'pg_transactional_inbox_slot',
    func: getEnvVariableString,
    skipFallback: true,
  },
];

const outboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'DB_PUBLICATION',
    default: 'pg_transactional_outbox_pub',
    func: getEnvVariableString,
    skipFallback: true,
  },
  {
    constantName: 'DB_REPLICATION_SLOT',
    default: 'pg_transactional_outbox_slot',
    func: getEnvVariableString,
    skipFallback: true,
  },
];

/**
 * Loads the environment variables into the replication listener settings
 * object. It supports reading an inbox specific setting or a general one.
 * Please use the `printInboxReplicationListenerEnvVariables` functions to get a
 * list of all the inbox relevant settings for the replication listener.
 * @example
 * TRX_DB_SCHEMA=trx_schema
 * TRX_INBOX_DB_TABLE=inbox_table
 * TRX_INBOX_DB_REPLICATION_SLOT=pg_transactional_inbox_slot
 * @param env The process.env variable or a custom object.
 * @returns The replication listener settings object with filled with the ENV variables
 */
export const getInboxReplicationListenerSettings = (
  env: Env = process.env,
): ReplicationListenerSettings => {
  const inboxSettings = getInboxListenerSettings(env);
  const repSettings = getConfigSettings(
    [...basicSettingsMap, ...inboxSettingsMap],
    inboxEnvPrefix,
    fallbackEnvPrefix,
    env,
  );
  return {
    ...inboxSettings,
    ...repSettings,
  } as unknown as ReplicationListenerSettings;
};

/**
 * Loads the environment variables into the replication listener settings
 * object. It supports reading an outbox specific setting or a general one.
 * Please use the `printOutboxReplicationListenerEnvVariables` functions to get a
 * list of all the outbox relevant settings for the replication listener.
 * @example
 * TRX_DB_SCHEMA=trx_schema
 * TRX_OUTBOX_DB_TABLE=outbox_table
 * TRX_OUTBOX_DB_REPLICATION_SLOT=pg_transactional_outbox_slot
 * @param env The process.env variable or a custom object.
 * @returns The replication listener settings object with filled with the ENV variables
 */
export const getOutboxReplicationListenerSettings = (
  env: Env = process.env,
): ReplicationListenerSettings => {
  const outboxSettings = getOutboxListenerSettings(env);
  const repSettings = getConfigSettings(
    [...basicSettingsMap, ...outboxSettingsMap],
    outboxEnvPrefix,
    fallbackEnvPrefix,
    env,
  );
  return {
    ...outboxSettings,
    ...repSettings,
  } as unknown as ReplicationListenerSettings;
};

/**
 * Shows the available env variables and their default values for the inbox
 * listener with the replication approach.
 * @param map A mapping of all the env variables to config settings.
 * @param envPrefix The prefix for the env variables to check first (e.g. "TRX_INBOX_" or "TRX_").
 * @param envPrefixFallback The fallback prefix if the other is not found. Useful for defining settings that should be used for both outbox and inbox.
 * @returns
 */
export const printInboxReplicationListenerEnvVariables = (
  defaultOverrides?: Record<string, string>,
): string => {
  const il = printInboxListenerEnvVariables(defaultOverrides);
  const cfg = printConfigSettings(
    [...basicSettingsMap, ...inboxSettingsMap],
    inboxEnvPrefix,
    fallbackEnvPrefix,
    defaultOverrides,
  );
  return `# Inbox listener variables
${il}
# Inbox replication listener variables
${cfg}`;
};

/**
 * Shows the available env variables and their default values for the outbox
 * listener with the replication approach.
 * @param map A mapping of all the env variables to config settings.
 * @param envPrefix The prefix for the env variables to check first (e.g. "TRX_OUTBOX_" or "TRX_").
 * @param envPrefixFallback The fallback prefix if the other is not found. Useful for defining settings that should be used for both outbox and inbox.
 * @returns
 */
export const printOutboxReplicationListenerEnvVariables = (
  defaultOverrides?: Record<string, string>,
): string => {
  const ol = printOutboxListenerEnvVariables(defaultOverrides);
  const cfg = printConfigSettings(
    [...basicSettingsMap, ...outboxSettingsMap],
    outboxEnvPrefix,
    fallbackEnvPrefix,
    defaultOverrides,
  );

  return `# Outbox listener variables
${ol}
# Outbox replication listener variables
${cfg}`;
};

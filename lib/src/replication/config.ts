import {
  BooleanSetting,
  Env,
  NumberSetting,
  StringSetting,
  getConfigSettings,
  getConfigSettingsEnvTemplate,
  getEnvVariableNumber,
  getEnvVariableString,
} from '../common/env-settings';
import {
  ListenerConfig,
  ListenerSettings,
  applyDefaultListenerConfigValues,
  fallbackEnvPrefix,
  getInboxListenerEnvTemplate,
  getInboxListenerSettings,
  getOutboxListenerEnvTemplate,
  getOutboxListenerSettings,
  inboxEnvPrefix,
  outboxEnvPrefix,
} from '../common/listener-config';

export type FullReplicationListenerConfig =
  Required<ReplicationListenerConfig> & {
    settings: FullReplicationListenerSettings;
  };

export type FullReplicationListenerSettings =
  Required<ReplicationListenerSettings>;

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

const defaultSettings: Required<
  Omit<
    ReplicationListenerSettings,
    keyof ListenerSettings | 'dbPublication' | 'dbReplicationSlot'
  >
> = {
  restartDelayInMs: 250,
  restartDelaySlotInUseInMs: 10_000,
};

export const applyDefaultReplicationListenerConfigValues = (
  config: ReplicationListenerConfig,
): FullReplicationListenerConfig => {
  const listenerConfig = applyDefaultListenerConfigValues(config);
  const filledConfig: FullReplicationListenerConfig = {
    ...listenerConfig,
    ...config,
    settings: {
      ...listenerConfig.settings,
      ...defaultSettings,
      ...config.settings,
    },
  };

  return filledConfig;
};

const basicSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'RESTART_DELAY_IN_MS',
    default: defaultSettings.restartDelayInMs,
    func: getEnvVariableNumber,
    description:
      'When there is a message handling error, how long the listener should wait to restart the processing.',
  },
  {
    constantName: 'RESTART_DELAY_SLOT_IN_USE_IN_MS',
    default: defaultSettings.restartDelaySlotInUseInMs,
    func: getEnvVariableNumber,
    description:
      'If the replication slot is in used, how long the listener should wait to connect again.',
  },
];

const outboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'DB_PUBLICATION',
    default: 'pg_transactional_outbox_pub',
    func: getEnvVariableString,
    skipFallback: true,
    description:
      'The name of the PostgreSQL publication that should be used for the outbox.',
  },
  {
    constantName: 'DB_REPLICATION_SLOT',
    default: 'pg_transactional_outbox_slot',
    func: getEnvVariableString,
    skipFallback: true,
    description:
      'The name of the PostgreSQL replication slot that should be used for the outbox.',
  },
];

const inboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'DB_PUBLICATION',
    default: 'pg_transactional_inbox_pub',
    func: getEnvVariableString,
    skipFallback: true,
    description:
      'The name of the PostgreSQL publication that should be used for the inbox.',
  },
  {
    constantName: 'DB_REPLICATION_SLOT',
    default: 'pg_transactional_inbox_slot',
    func: getEnvVariableString,
    skipFallback: true,
    description:
      'The name of the PostgreSQL replication slot that should be used for the inbox.',
  },
];

/**
 * Loads the environment variables into the replication listener settings
 * object. It supports reading an inbox specific setting or a general one.
 * Please use the `getInboxReplicationListenerEnvTemplate` functions to get a
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
 * Please use the `getOutboxReplicationListenerEnvTemplate` functions to get a
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
export const getInboxReplicationListenerEnvTemplate = (
  defaultOverrides?: Record<string, string>,
): string => {
  const il = getInboxListenerEnvTemplate(defaultOverrides);
  const cfg = getConfigSettingsEnvTemplate(
    [...basicSettingsMap, ...inboxSettingsMap],
    inboxEnvPrefix,
    fallbackEnvPrefix,
    defaultOverrides,
  );
  return `${il}
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
export const getOutboxReplicationListenerEnvTemplate = (
  defaultOverrides?: Record<string, string>,
): string => {
  const ol = getOutboxListenerEnvTemplate(defaultOverrides);
  const cfg = getConfigSettingsEnvTemplate(
    [...basicSettingsMap, ...outboxSettingsMap],
    outboxEnvPrefix,
    fallbackEnvPrefix,
    defaultOverrides,
  );

  return `${ol}
${cfg}`;
};

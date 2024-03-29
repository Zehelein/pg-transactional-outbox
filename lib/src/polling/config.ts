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

export type FullPollingListenerConfig = Required<PollingListenerConfig> & {
  settings: FullPollingListenerSettings;
};

export type FullPollingListenerSettings = Required<PollingListenerSettings>;

export interface PollingListenerConfig extends ListenerConfig {
  /** Polling listener specific settings */
  settings: PollingListenerSettings;
}

export interface PollingListenerSettings extends ListenerSettings {
  /**
   * The name of the schema of the Postgres function to get the next batch of
   * messages. It defaults to the `dbSchema` if it is not provided.
   */
  nextMessagesFunctionSchema?: string;
  /**
   * The name of the Postgres function to get the next batch of outbox or inbox
   * messages.
   */
  nextMessagesFunctionName: string;
  /** The batch size for messages to load simultaneously. Default is 5. */
  nextMessagesBatchSize?: number;
  /** How long should a message be locked for exclusive processing and error handling (in milliseconds). Default is 5 seconds. */
  nextMessagesLockInMs?: number;
  /** Next polling interval. Default is 500ms. */
  nextMessagesPollingIntervalInMs?: number;
}

const defaultSettings: Required<
  Omit<
    PollingListenerSettings,
    keyof ListenerSettings | 'nextMessagesFunctionName'
  >
> = {
  nextMessagesFunctionSchema: 'public',
  nextMessagesBatchSize: 5,
  nextMessagesLockInMs: 5000,
  nextMessagesPollingIntervalInMs: 500,
};

export const applyDefaultPollingListenerConfigValues = (
  config: PollingListenerConfig,
): FullPollingListenerConfig => {
  const listenerConfig = applyDefaultListenerConfigValues(config);
  const filledConfig: FullPollingListenerConfig = {
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
    constantName: 'NEXT_MESSAGES_FUNCTION_SCHEMA',
    default: defaultSettings.nextMessagesFunctionSchema,
    func: getEnvVariableString,
    description: 'The database schema of the next messages function.',
  },
  {
    constantName: 'NEXT_MESSAGES_BATCH_SIZE',
    default: defaultSettings.nextMessagesBatchSize,
    func: getEnvVariableNumber,
    description: 'The (maximum) amount of messages to retrieve in one query.',
  },
  {
    constantName: 'NEXT_MESSAGES_LOCK_IN_MS',
    default: defaultSettings.nextMessagesLockInMs,
    func: getEnvVariableNumber,
    description:
      'How long the retrieved messages should be locked before they can be retrieved again.',
  },
  {
    constantName: 'NEXT_MESSAGES_POLLING_INTERVAL_IN_MS',
    default: defaultSettings.nextMessagesPollingIntervalInMs,
    func: getEnvVariableNumber,
    description: 'How often should the next messages function be executed.',
  },
];

const outboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'NEXT_MESSAGES_FUNCTION_NAME',
    default: 'next_outbox_messages',
    func: getEnvVariableString,
    skipFallback: true,
    description:
      'The database function name to get the next batch of outbox messages.',
  },
];

const inboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'NEXT_MESSAGES_FUNCTION_NAME',
    default: 'next_inbox_messages',
    func: getEnvVariableString,
    skipFallback: true,
    description:
      'The database function name to get the next batch of inbox messages.',
  },
];

/**
 * Loads the environment variables into the polling listener settings object. It
 * supports reading an inbox specific setting or a general one.
 * Please use the `getInboxPollingListenerEnvTemplate` functions to get a
 * list of all the inbox relevant settings for the polling listener.
 * @example
 * TRX_DB_SCHEMA=trx_schema
 * TRX_INBOX_DB_TABLE=inbox_table
 * TRX_INBOX_NEXT_MESSAGES_FUNCTION_SCHEMA=next_inbox_messages
 * @param env The process.env variable or a custom object.
 * @returns The polling listener settings object with filled with the ENV variables
 */
export const getInboxPollingListenerSettings = (
  env: Env = process.env,
): PollingListenerSettings => {
  const inboxSettings = getInboxListenerSettings(env);
  const pollSettings = getConfigSettings(
    [...basicSettingsMap, ...inboxSettingsMap],
    inboxEnvPrefix,
    fallbackEnvPrefix,
    env,
  );
  return {
    ...inboxSettings,
    ...pollSettings,
  } as unknown as PollingListenerSettings;
};

/**
 * Loads the environment variables into the polling listener settings object. It
 * supports reading an outbox specific setting or a general one.
 * Please use the `getOutboxPollingListenerEnvTemplate` functions to get a
 * list of all the outbox relevant settings for the polling listener.
 * @example
 * TRX_DB_SCHEMA=trx_schema
 * TRX_OUTBOX_DB_TABLE=outbox_table
 * TRX_OUTBOX_NEXT_MESSAGES_FUNCTION_SCHEMA=next_outbox_messages
 * @param env The process.env variable or a custom object.
 * @returns The polling listener settings object with filled with the ENV variables
 */
export const getOutboxPollingListenerSettings = (
  env: Env = process.env,
): PollingListenerSettings => {
  const outboxSettings = getOutboxListenerSettings(env);
  const pollSettings = getConfigSettings(
    [...basicSettingsMap, ...outboxSettingsMap],
    outboxEnvPrefix,
    fallbackEnvPrefix,
    env,
  );
  return {
    ...outboxSettings,
    ...pollSettings,
  } as unknown as PollingListenerSettings;
};

/**
 * Shows the available env variables and their default values for the inbox
 * listener with the polling approach.
 * @param map A mapping of all the env variables to config settings.
 * @param envPrefix The prefix for the env variables to check first (e.g. "TRX_INBOX_" or "TRX_").
 * @param envPrefixFallback The fallback prefix if the other is not found. Useful for defining settings that should be used for both outbox and inbox.
 * @returns
 */
export const getInboxPollingListenerEnvTemplate = (
  defaultOverrides?: Record<string, string>,
): string => {
  const il = getInboxListenerEnvTemplate(defaultOverrides);
  const cfg = getConfigSettingsEnvTemplate(
    [...basicSettingsMap, ...inboxSettingsMap],
    inboxEnvPrefix,
    fallbackEnvPrefix,
    defaultOverrides,
  );
  return `${il}${cfg}`;
};
/**
 * Shows the available env variables and their default values for the outbox
 * listener with the polling approach.
 * @param map A mapping of all the env variables to config settings.
 * @param envPrefix The prefix for the env variables to check first (e.g. "TRX_OUTBOX_" or "TRX_").
 * @param envPrefixFallback The fallback prefix if the other is not found. Useful for defining settings that should be used for both outbox and inbox.
 * @returns
 */
export const getOutboxPollingListenerEnvTemplate = (
  defaultOverrides?: Record<string, string>,
): string => {
  const ol = getOutboxListenerEnvTemplate(defaultOverrides);
  const cfg = getConfigSettingsEnvTemplate(
    [...basicSettingsMap, ...outboxSettingsMap],
    outboxEnvPrefix,
    fallbackEnvPrefix,
    defaultOverrides,
  );
  return `${ol}${cfg}`;
};

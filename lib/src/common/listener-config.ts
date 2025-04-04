import { ClientConfig } from 'pg';
import {
  BooleanSetting,
  Env,
  NumberSetting,
  StringSetting,
  getConfigSettings,
  getConfigSettingsEnvTemplate,
  getEnvVariableBoolean,
  getEnvVariableNumber,
  getEnvVariableString,
} from './env-settings';

export type OutboxOrInbox = 'outbox' | 'inbox';

export type FullListenerConfig = Required<ListenerConfig> & {
  settings: FullListenerSettings;
};

export type FullListenerSettings = Required<ListenerSettings>;

export interface ListenerConfig {
  /**
   * Defines if this listener is used for the transactional outbox or inbox
   * handling
   */
  outboxOrInbox: OutboxOrInbox;
  /**
   * Database connection details for the message handler logic. The user needs
   * update permission to the outbox or inbox table. Uses the dbListenerConfig
   * if it is not provided.
   */
  dbHandlerConfig?: ClientConfig;
  /**
   * The "pg" library based settings to initialize the PostgreSQL connection for
   * the logical replication listener (with replication permissions) or the
   * polling listener.
   */
  dbListenerConfig: ClientConfig;
  /** Listener specific configurations */
  settings: ListenerSettings;
}

export interface ListenerSettings {
  /** The database schema name where the table is located */
  dbSchema: string;
  /** The database table of the outbox/inbox */
  dbTable: string;
  /**
   * Outbox message sender or the inbox message handlers that do not finish can
   * block further messages from being processed/sent. The timeout
   * (in milliseconds) ensures to continue with the next items. Default is 15s.
   */
  messageProcessingTimeoutInMs?: number;
  /**
   * The maximum number of attempts to handle an incoming message.
   * Defaults to 5 which means a message is handled once initially and up to
   * four more times for retries.
   */
  maxAttempts?: number;
  /**
   * Enable max poisonous attempts protection. Might be disabled when using it for the
   * outbox scenario. Defaults to true.
   */
  enableMaxAttemptsProtection: boolean;
  /**
   * Defines the maximum number of times a message is going to be processed
   * when it is (likely) a poisonous message which is causing a server crash.
   * This is used in the default poisonous message retry strategy.
   * It defaults to three.
   */
  maxPoisonousAttempts?: number;
  /**
   * Poisonous message protection is enabled by default or if you set this to
   * true. Enabling it will take a little bit more time but will prevent an
   * infinite service crash loop if there is a poisonous message.
   */
  enablePoisonousMessageProtection: boolean;
  /**
   * The maximum number of retries when a message could not be found.
   * Defaults to 0 which means it tries to find the message only once but will
   * not try it again.
   */
  maxMessageNotFoundAttempts?: number;
  /**
   * The time to wait before trying to find a not (yet) found message again.
   * Defaults to 10ms.
   */
  maxMessageNotFoundDelayInMs?: number;
  /**
   * Time in milliseconds between the execution of the old message cleanups.
   * Leave it undefined or zero to disable automatic message cleanup.
   */
  messageCleanupIntervalInMs?: number;
  /** Delete messages where the processed field is older than this in seconds */
  messageCleanupProcessedInSec?: number;
  /** Delete messages where the abandoned field is older than this in seconds */
  messageCleanupAbandonedInSec?: number;
  /** Delete messages where the created field is older than this in seconds */
  messageCleanupAllInSec?: number;
}

const defaultSettings: Omit<FullListenerSettings, 'dbTable'> = {
  dbSchema: 'public',
  messageProcessingTimeoutInMs: 15_000,
  maxAttempts: 5,
  enableMaxAttemptsProtection: true,
  maxPoisonousAttempts: 3,
  enablePoisonousMessageProtection: true,
  maxMessageNotFoundAttempts: 0,
  maxMessageNotFoundDelayInMs: 10,
  messageCleanupIntervalInMs: 5 * 60 * 1000,
  messageCleanupProcessedInSec: 7 * 24 * 60 * 60,
  messageCleanupAbandonedInSec: 14 * 24 * 60 * 60,
  messageCleanupAllInSec: 60 * 24 * 60 * 60,
};

export const applyDefaultListenerConfigValues = (
  config: ListenerConfig,
): FullListenerConfig => {
  const filledConfig: FullListenerConfig = {
    ...config,
    dbHandlerConfig: config.dbHandlerConfig ?? config.dbListenerConfig,
    settings: {
      ...defaultSettings,
      ...config.settings,
    },
  };

  return filledConfig;
};

const basicSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'DB_SCHEMA',
    default: defaultSettings.dbSchema,
    func: getEnvVariableString,
    description: 'The database schema name where the table is located.',
  },
  {
    constantName: 'MESSAGE_PROCESSING_TIMEOUT_IN_MS',
    default: defaultSettings.messageProcessingTimeoutInMs,
    func: getEnvVariableNumber,
    description: 'Stop the message handler after this time has passed.',
  },
  {
    constantName: 'MAX_ATTEMPTS',
    func: getEnvVariableNumber,
    default: defaultSettings.maxAttempts,
    description:
      'The maximum number of attempts to handle a message. With max 5 attempts a message is handled once initially and up to four more times for retries.',
  },
  {
    constantName: 'MAX_POISONOUS_ATTEMPTS',
    default: defaultSettings.maxPoisonousAttempts,
    func: getEnvVariableNumber,
    description:
      'The maximum number of times a message should be attempted which was started but did not finish (neither error nor success).',
  },
  {
    constantName: 'MESSAGE_CLEANUP_INTERVAL_IN_MS',
    default: defaultSettings.messageCleanupIntervalInMs,
    func: getEnvVariableNumber,
    description:
      'Time in milliseconds between the execution of the old message cleanups. Set it to zero to disable automatic message cleanup.',
  },
  {
    constantName: 'MESSAGE_CLEANUP_PROCESSED_IN_SEC',
    default: defaultSettings.messageCleanupProcessedInSec,
    func: getEnvVariableNumber,
    description:
      'Delete messages that were successfully processed after X seconds.',
  },
  {
    constantName: 'MESSAGE_CLEANUP_ABANDONED_IN_SEC',
    default: defaultSettings.messageCleanupAbandonedInSec,
    func: getEnvVariableNumber,
    description: 'Delete messages that could not be processed after X seconds.',
  },
  {
    constantName: 'MESSAGE_CLEANUP_ALL_IN_SEC',
    default: defaultSettings.messageCleanupAllInSec,
    func: getEnvVariableNumber,
    description: 'Delete all old messages after X seconds.',
  },
];

const outboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'DB_TABLE',
    default: 'outbox',
    func: getEnvVariableString,
    skipFallback: true,
    description: 'The name of the database outbox table.',
  },
  {
    constantName: 'ENABLE_MAX_ATTEMPTS_PROTECTION',
    default: false,
    func: getEnvVariableBoolean,
    skipFallback: true,
    description: 'Enable the max attempts protection.',
  },
  {
    constantName: 'ENABLE_POISONOUS_MESSAGE_PROTECTION',
    default: false,
    func: getEnvVariableBoolean,
    skipFallback: true,
    description: 'Enable the max poisonous attempts protection.',
  },
];

const inboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] = [
  {
    constantName: 'DB_TABLE',
    default: 'inbox',
    func: getEnvVariableString,
    skipFallback: true,
    description: 'The name of the database inbox table.',
  },
  {
    constantName: 'ENABLE_MAX_ATTEMPTS_PROTECTION',
    default: true,
    func: getEnvVariableBoolean,
    skipFallback: true,
    description: 'Enable the max attempts protection.',
  },
  {
    constantName: 'ENABLE_POISONOUS_MESSAGE_PROTECTION',
    default: true,
    func: getEnvVariableBoolean,
    skipFallback: true,
    description: 'Enable the max poisonous attempts protection.',
  },
];

export const fallbackEnvPrefix = 'TRX_';
export const inboxEnvPrefix = 'TRX_INBOX_';
export const outboxEnvPrefix = 'TRX_OUTBOX_';

/**
 * Loads the environment variables into the listener settings object. It
 * supports reading an inbox specific setting or a general one.
 * Please use the `getXxxxListenerEnvTemplate` functions to get a list of all
 * the outbox and inbox relevant settings for the replication or polling
 * listener.
 * @example
 * TRX_DB_SCHEMA=trx_schema
 * TRX_INBOX_DB_TABLE=inbox_table
 * TRX_MESSAGE_PROCESSING_TIMEOUT_IN_MS=30000
 * @param env The process.env variable or a custom object.
 * @returns The listener settings object with filled with the ENV variables
 */
export const getInboxListenerSettings = (
  env: Env = process.env,
): ListenerSettings =>
  getConfigSettings(
    [...basicSettingsMap, ...inboxSettingsMap],
    inboxEnvPrefix,
    fallbackEnvPrefix,
    env,
  ) as unknown as ListenerSettings;

/**
 * Loads the environment variables into the listener settings object. It
 * supports reading an outbox specific setting or a general one.
 * Please use the `getXxxxListenerEnvTemplate` functions to get a list of all
 * the outbox and outbox relevant settings for the replication or polling
 * listener.
 * @example
 * TRX_DB_SCHEMA=trx_schema
 * TRX_OUTBOX_DB_TABLE=outbox_table
 * TRX_MESSAGE_PROCESSING_TIMEOUT_IN_MS=30000
 * @param env The process.env variable or a custom object.
 * @returns The listener settings object with filled with the ENV variables
 */
export const getOutboxListenerSettings = (
  env: Env = process.env,
): ListenerSettings =>
  getConfigSettings(
    [...basicSettingsMap, ...outboxSettingsMap],
    outboxEnvPrefix,
    fallbackEnvPrefix,
    env,
  ) as unknown as ListenerSettings;

/**
 * Creates the available env variables and their default values for the basic
 * inbox listener env variables.
 */
export const getInboxListenerEnvTemplate = (
  defaultOverrides?: Record<string, string>,
): string =>
  getConfigSettingsEnvTemplate(
    [...basicSettingsMap, ...inboxSettingsMap],
    inboxEnvPrefix,
    fallbackEnvPrefix,
    defaultOverrides,
  );

/**
 * Creates the available env variables and their default values for the basic
 * outbox listener env variables.
 */
export const getOutboxListenerEnvTemplate = (
  defaultOverrides?: Record<string, string>,
): string =>
  getConfigSettingsEnvTemplate(
    [...basicSettingsMap, ...outboxSettingsMap],
    outboxEnvPrefix,
    fallbackEnvPrefix,
    defaultOverrides,
  );

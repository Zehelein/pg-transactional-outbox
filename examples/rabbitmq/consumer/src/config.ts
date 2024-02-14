import {
  DatabasePollingSetupConfig,
  DatabaseReplicationSetupConfig,
  PollingListenerConfig,
  ReplicationListenerConfig,
  getInboxPollingListenerSettings,
  getInboxReplicationListenerSettings,
} from 'pg-transactional-outbox';

/**
 * Parses the environment or provided settings and ensures all the fields are
 * set with provided or default data.
 * @param env The process environment settings or a custom settings object
 * @returns
 */
// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export const getConfig = (env: Env = process.env) => {
  return {
    postgresHost: getEnvVariableString(env, 'POSTGRESQL_HOST', 'localhost'),
    postgresPort: getEnvVariableNumber(env, 'POSTGRESQL_PORT', 5432),
    postgresDatabase: getEnvVariableString(
      env,
      'POSTGRESQL_DATABASE',
      'pg_transactional_inbox',
    ),
    postgresInboxListenerRole: getEnvVariableString(
      env,
      'POSTGRESQL_INBOX_LISTENER_ROLE',
      'db_inbox_listener',
    ),
    postgresInboxListenerRolePassword: getEnvVariableString(
      env,
      'POSTGRESQL_INBOX_LISTENER_ROLE_PASSWORD',
      'db_inbox_listener_password',
    ),
    postgresHandlerRole: getEnvVariableString(
      env,
      'POSTGRESQL_INBOX_HANDLER_ROLE',
      'db_inbox_handler',
    ),
    postgresHandlerRolePassword: getEnvVariableString(
      env,
      'POSTGRESQL_INBOX_HANDLER_ROLE_PASSWORD',
      'db_inbox_handler_password',
    ),
    listenerType: getEnvVariableString(env, 'LISTENER_TYPE', 'replication'),

    rmqProtocol: getEnvVariableString(env, 'RABBITMQ_PROTOCOL', 'amqp'),
    rmqHost: getEnvVariableString(env, 'RABBITMQ_HOST', 'localhost'),
    rmqPort: getEnvVariableNumber(env, 'RABBITMQ_PORT', 5672),
    rmqVHost: getEnvVariableString(env, 'RABBITMQ_VHOST', '/'),
    rmqUser: getEnvVariableString(env, 'RABBITMQ_USER', 'guest'),
    rmqPassword: getEnvVariableString(env, 'RABBITMQ_PASSWORD', 'guest'),
    rmqChannelMax: getEnvVariableNumber(env, 'RABBITMQ_CHANNEL_MAX', 100),

    // Management properties are optional - they are only used for VHost assertion (creation).
    rmqMgmtProtocol: getEnvVariableString(
      env,
      'RABBITMQ_MGMT_PROTOCOL',
      'http',
    ),
    rmqMgmtHost: getEnvVariableString(env, 'RABBITMQ_MGMT_HOST', 'localhost'),
    rmqMgmtPort: getEnvVariableNumber(env, 'RABBITMQ_MGMT_PORT', 15672),

    maxAttempts: getEnvVariableNumber(env, 'MAX_ATTEMPTS', 5),
    maxPoisonousAttempts: getEnvVariableNumber(
      env,
      'MAX_POISONOUS_ATTEMPTS',
      3,
    ),
  };
};

/** The configuration object type with parsed environment variables. */
export type Config = ReturnType<typeof getConfig>;

const getDbConnections = (config: Config) => ({
  dbHandlerConfig: {
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresHandlerRole,
    password: config.postgresHandlerRolePassword,
    database: config.postgresDatabase,
  },
  dbListenerConfig: {
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresInboxListenerRole,
    password: config.postgresInboxListenerRolePassword,
    database: config.postgresDatabase,
  },
});

export const getReplicationInboxConfig = (
  config: Config,
): ReplicationListenerConfig => {
  return {
    outboxOrInbox: 'inbox',
    ...getDbConnections(config),
    settings: getInboxReplicationListenerSettings(),
  };
};

export const getPollingInboxConfig = (
  config: Config,
): PollingListenerConfig => {
  return {
    outboxOrInbox: 'inbox',
    ...getDbConnections(config),
    settings: getInboxPollingListenerSettings(),
  };
};

export const getDatabaseSetupConfig = (
  config: Config,
): DatabaseReplicationSetupConfig & DatabasePollingSetupConfig => {
  const repSettings = getInboxReplicationListenerSettings();
  const pollSettings = getInboxPollingListenerSettings();
  return {
    outboxOrInbox: 'inbox',
    database: config.postgresDatabase,
    schema: repSettings.dbSchema,
    table: repSettings.dbTable,
    listenerRole: config.postgresInboxListenerRole,
    handlerRole: config.postgresHandlerRole,
    // Replication
    replicationSlot: repSettings.dbReplicationSlot,
    publication: repSettings.dbPublication,
    // Polling
    nextMessagesName: pollSettings.nextMessagesFunctionName,
    nextMessagesSchema: pollSettings.nextMessagesFunctionSchema,
  };
};

interface Env {
  [key: string]: string | undefined;
}

function getEnvVariableString(
  env: Env,
  field: string,
  defaultValue?: string,
): string {
  const value = env[field] ?? defaultValue;
  if (typeof value !== 'string' || !value) {
    throw new Error(
      `The environment variable ${field} must be a non-empty string.`,
    );
  }
  return value;
}

function getEnvVariableNumber(
  env: Env,
  field: string,
  defaultValue?: number,
): number {
  const value = Number(getEnvVariableString(env, field, `${defaultValue}`));
  if (Number.isNaN(value)) {
    throw new Error(`The environment variable ${field} must be a number.`);
  }
  return value;
}

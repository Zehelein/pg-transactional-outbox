import {
  DatabasePollingSetupConfig,
  DatabaseReplicationSetupConfig,
  PollingListenerConfig,
  ReplicationListenerConfig,
  getOutboxPollingListenerSettings,
  getOutboxReplicationListenerSettings,
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
      'pg_transactional_outbox',
    ),
    postgresOutboxListenerRole: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_LISTENER_ROLE',
      'db_outbox_listener',
    ),
    postgresOutboxListenerRolePassword: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_LISTENER_ROLE_PASSWORD',
      'db_outbox_listener_password',
    ),

    postgresHandlerRole: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_HANDLER_ROLE',
      'db_outbox_handler',
    ),
    postgresHandlerRolePassword: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_HANDLER_ROLE_PASSWORD',
      'db_outbox_handler_password',
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
    user: config.postgresOutboxListenerRole,
    password: config.postgresOutboxListenerRolePassword,
    database: config.postgresDatabase,
  },
});

export const getReplicationOutboxConfig = (
  config: Config,
): ReplicationListenerConfig => {
  return {
    outboxOrInbox: 'outbox',
    ...getDbConnections(config),
    settings: getOutboxReplicationListenerSettings(),
  };
};

export const getPollingOutboxConfig = (
  config: Config,
): PollingListenerConfig => {
  return {
    outboxOrInbox: 'outbox',
    ...getDbConnections(config),
    settings: getOutboxPollingListenerSettings(),
  };
};

export const getDatabaseSetupConfig = (
  config: Config,
): DatabaseReplicationSetupConfig & DatabasePollingSetupConfig => {
  const repSettings = getOutboxReplicationListenerSettings();
  const pollSettings = getOutboxPollingListenerSettings();

  return {
    outboxOrInbox: 'outbox',
    database: config.postgresDatabase,
    schema: repSettings.dbSchema,
    table: repSettings.dbTable,
    listenerRole: config.postgresOutboxListenerRole,
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

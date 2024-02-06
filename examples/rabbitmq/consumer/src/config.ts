import { PollingConfig, ReplicationConfig } from 'pg-transactional-outbox';

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

    postgresInboxSchema: getEnvVariableString(
      env,
      'POSTGRESQL_INBOX_SCHEMA',
      'inbox',
    ),
    postgresInboxTable: getEnvVariableString(
      env,
      'POSTGRESQL_INBOX_TABLE',
      'inbox',
    ),
    postgresInboxSlot: getEnvVariableString(
      env,
      'POSTGRESQL_INBOX_SLOT',
      'pg_transactional_inbox_slot',
    ),
    postgresInboxPub: getEnvVariableString(
      env,
      'POSTGRESQL_INBOX_PUB',
      'pg_transactional_inbox_pub',
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
    nextInboxMessagesFunctionName: getEnvVariableString(
      env,
      'NEXT_INBOX_MESSAGES_FUNCTION_NAME',
      'next_inbox_messages',
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

export const getReplicationInboxConfig = (
  config: Config,
): ReplicationConfig => {
  return {
    outboxOrInbox: 'inbox',
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
    settings: {
      dbSchema: config.postgresInboxSchema,
      dbTable: config.postgresInboxTable,
      postgresPub: config.postgresInboxPub,
      postgresSlot: config.postgresInboxSlot,
      enableMaxAttemptsProtection: true,
      maxAttempts: config.maxAttempts,
      enablePoisonousMessageProtection: true,
      maxPoisonousAttempts: config.maxPoisonousAttempts,
    },
  };
};

export const getPollingInboxConfig = (config: Config): PollingConfig => {
  const repConfig = getReplicationInboxConfig(config);
  return {
    outboxOrInbox: 'inbox',
    dbHandlerConfig: repConfig.dbHandlerConfig,
    dbListenerConfig: repConfig.dbListenerConfig,
    settings: {
      dbSchema: config.postgresInboxSchema,
      dbTable: config.postgresInboxTable,
      enableMaxAttemptsProtection: true,
      maxAttempts: config.maxAttempts,
      enablePoisonousMessageProtection: true,
      maxPoisonousAttempts: config.maxPoisonousAttempts,
      nextMessagesBatchSize: 5,
      nextMessagesFunctionName: config.nextInboxMessagesFunctionName,
      nextMessagesPollingInterval: 500,
    },
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

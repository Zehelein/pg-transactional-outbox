import {
  DatabasePollingSetupConfig,
  DatabaseReplicationSetupConfig,
  PollingConfig,
  ReplicationConfig,
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

    postgresOutboxSchema: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_SCHEMA',
      'outbox',
    ),
    postgresOutboxTable: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_TABLE',
      'outbox',
    ),
    postgresOutboxSlot: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_SLOT',
      'pg_transactional_outbox_slot',
    ),
    postgresOutboxPub: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_PUB',
      'pg_transactional_outbox_pub',
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
    nextOutboxMessagesFunctionName: getEnvVariableString(
      env,
      'NEXT_OUTBOX_MESSAGES_FUNCTION_NAME',
      'next_outbox_messages',
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

export const getReplicationOutboxConfig = (
  config: Config,
): ReplicationConfig => {
  return {
    outboxOrInbox: 'outbox',
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
    settings: {
      dbSchema: config.postgresOutboxSchema,
      dbTable: config.postgresOutboxTable,
      postgresPub: config.postgresOutboxPub,
      postgresSlot: config.postgresOutboxSlot,
      // For the outbox we skip those settings as we assume sending the message will succeed (once RabbitMQ is up again)
      enableMaxAttemptsProtection: false,
      enablePoisonousMessageProtection: false,
    },
  };
};

export const getPollingOutboxConfig = (config: Config): PollingConfig => {
  const repConfig = getReplicationOutboxConfig(config);
  return {
    outboxOrInbox: 'outbox',
    dbHandlerConfig: repConfig.dbHandlerConfig,
    dbListenerConfig: repConfig.dbListenerConfig,
    settings: {
      dbSchema: config.postgresOutboxSchema,
      dbTable: config.postgresOutboxTable,
      // For the outbox we skip those settings as we assume sending the message will succeed (once RabbitMQ is up again)
      enableMaxAttemptsProtection: false,
      enablePoisonousMessageProtection: false,
      nextMessagesBatchSize: 5,
      nextMessagesFunctionName: config.nextOutboxMessagesFunctionName,
      nextMessagesPollingInterval: 500,
    },
  };
};

export const getDatabaseSetupConfig = (
  config: Config,
): DatabaseReplicationSetupConfig & DatabasePollingSetupConfig => {
  return {
    outboxOrInbox: 'outbox',
    database: config.postgresDatabase,
    schema: config.postgresOutboxSchema,
    table: config.postgresOutboxTable,
    listenerRole: config.postgresOutboxListenerRole,
    handlerRole: config.postgresHandlerRole,
    // Replication
    replicationSlot: config.postgresOutboxSlot,
    publication: config.postgresOutboxPub,
    // Polling
    nextMessagesName: config.nextOutboxMessagesFunctionName,
    nextMessagesSchema: config.postgresOutboxSchema,
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

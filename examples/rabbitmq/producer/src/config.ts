import { ServiceConfig } from 'pg-transactional-outbox';

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
    postgresOutboxRole: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_ROLE',
      'db_outbox',
    ),
    postgresOutboxRolePassword: getEnvVariableString(
      env,
      'POSTGRESQL_OUTBOX_ROLE_PASSWORD',
      'db_outbox_password',
    ),

    postgresLoginRole: getEnvVariableString(
      env,
      'POSTGRESQL_LOGIN_ROLE',
      'db_login_outbox',
    ),
    postgresLoginRolePassword: getEnvVariableString(
      env,
      'POSTGRESQL_LOGIN_ROLE_PASSWORD',
      'db_login_outbox_password',
    ),

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

export const getOutboxServiceConfig = (config: Config): ServiceConfig => {
  return {
    pgReplicationConfig: {
      host: config.postgresHost,
      port: config.postgresPort,
      user: config.postgresOutboxRole,
      password: config.postgresOutboxRolePassword,
      database: config.postgresDatabase,
    },
    settings: {
      dbSchema: config.postgresOutboxSchema,
      dbTable: config.postgresOutboxTable,
      postgresPub: config.postgresOutboxPub,
      postgresSlot: config.postgresOutboxSlot,
    },
  };
};

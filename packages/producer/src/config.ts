type Env = { [key: string]: string | undefined };

function getEnvVariableString(
  env: Env,
  field: string,
  defaultValue?: string,
): string {
  const value = env[field] ?? defaultValue;
  if (typeof value !== 'string' || !value)
    throw new Error(
      `The environment variable ${field} must be a non-empty string.`,
    );
  return value;
}

function getEnvVariableNumber(
  env: Env,
  field: string,
  defaultValue?: number,
): number {
  const value = Number(getEnvVariableString(env, field, `${defaultValue}`));
  if (Number.isNaN(value))
    throw new Error(`The environment variable ${field} must be a number.`);
  return value;
}

/**
 * Parses the environment or provided settings and ensures all the fields are
 * set with provided or default data.
 * @param env The process environment settings or a custom settings object
 * @returns
 */
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
      'db_login',
    ),
    postgresLoginRolePassword: getEnvVariableString(
      env,
      'POSTGRESQL_LOGIN_ROLE_PASSWORD',
      'db_login_password',
    ),
  };
};

/** The configuration object type with parsed environment variables. */
export type Config = ReturnType<typeof getConfig>;

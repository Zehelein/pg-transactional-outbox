/** The node.js environment variable interface */
export interface Env {
  [key: string]: string | undefined;
}

/**
 * Get a string from the environment variable.
 * @throws Error if the variable is not found or empty and no default value was provided.
 */
export const getEnvVariableString = (
  env: Env,
  field: string,
  fallbackField: string,
  defaultValue?: string,
): string => {
  const value = env[field] ?? env[fallbackField];
  if (typeof value !== 'string' || value === '') {
    if (defaultValue) {
      return defaultValue;
    }
    throw new Error(
      `The environment variable ${field} must be a non-empty string.`,
    );
  }
  return value;
};

/**
 * Get a string from the environment variable.
 * @throws Error if the variable is not found or empty and no default value was provided.
 */
export const getEnvVariableNumber = (
  env: Env,
  field: string,
  fallbackField: string,
  defaultValue?: number,
): number => {
  const value = Number(
    getEnvVariableString(env, field, fallbackField, `${defaultValue}`),
  );
  if (Number.isNaN(value)) {
    if (defaultValue) {
      return defaultValue;
    }
    throw new Error(`The environment variable ${field} must be a number.`);
  }
  return value;
};

/**
 * Get a boolean from the environment variable. The true/1 value return true, false/0 return false. Everything else throws an error.
 * @throws Error if the variable is not found or empty and no default value was provided.
 */
export const getEnvVariableBoolean = (
  env: Env,
  field: string,
  fallbackField: string,
  defaultValue?: boolean,
): boolean => {
  const value = getEnvVariableString(
    env,
    field,
    fallbackField,
    `${defaultValue}`,
  ).toLowerCase();
  if (value === 'true' || value === '1') {
    return true;
  }
  if (value === 'false' || value === '0') {
    return false;
  }
  if (defaultValue) {
    return defaultValue;
  }
  throw new Error(`The environment variable ${field} must be a number.`);
};

/**
 * Transforms a constant formatted value to snake case formatted one
 * @param constantStr The CONSTANT_VALUE to convert to constantValue
 * @returns the formatted value
 */
export const constantToCamel = (constantStr: string): string => {
  return constantStr
    .toLowerCase()
    .replace(/_([a-z0-9])/g, (_, char) => char.toUpperCase());
};

export interface StringSetting {
  constantName: string;
  default: string;
  func: typeof getEnvVariableString;
}

export interface NumberSetting {
  constantName: string;
  default: number;
  func: typeof getEnvVariableNumber;
}

export interface BooleanSetting {
  constantName: string;
  default: boolean;
  func: typeof getEnvVariableBoolean;
}

/**
 * Loads the configuration settings from the ENV variables into the settings object.
 * @param map A mapping of all the env variables to config settings.
 * @param envPrefix The prefix for the env variables to check first (e.g. "TRX_OUTBOX_" or "TRX_INBOX_").
 * @param envPrefixFallback The fallback prefix if the other is not found. Useful for defining settings that should be used for both outbox and inbox.
 * @param env The process.env variable or a custom object
 * @returns The parsed configuration object.
 */
export const getConfigSettings = (
  map: (StringSetting | NumberSetting | BooleanSetting)[],
  envPrefix: string,
  envPrefixFallback: string,
  env: Env = process.env,
): Record<string, string | number | boolean> => {
  const settings: Record<string, string | number | boolean> = {};
  for (const s of map) {
    const key = constantToCamel(s.constantName);

    settings[key] = s.func(
      env,
      `${envPrefix}${s.constantName}`,
      `${envPrefixFallback}${s.constantName}`,
      s.default as any,
    );
  }
  return settings;
};

/**
 * Shows the available env variables and their default values
 * @param map A mapping of all the env variables to config settings.
 * @param envPrefix The prefix for the env variables to check first (e.g. "TRX_OUTBOX_" or "TRX_INBOX_").
 * @param envPrefixFallback The fallback prefix if the other is not found. Useful for defining settings that should be used for both outbox and inbox.
 * @returns A string with all the ENV config keys and their default values.
 */
export const printConfigSettings = (
  map: (StringSetting | NumberSetting | BooleanSetting)[],
  envPrefix: string,
  envPrefixFallback: string,
): string => {
  let result = '';
  for (const s of map) {
    result += `${envPrefix}${s.constantName}=${s.default}
${envPrefixFallback}${s.constantName}=${s.default}
`;
  }
  return result;
};

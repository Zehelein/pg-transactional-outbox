/* eslint-disable @typescript-eslint/no-var-requires */
import pino, { BaseLogger } from 'pino';

let outboxLogger: BaseLogger = pino();

/**
 * Set a custom logger to use for logging. The default is using a plain pino logger.
 * @param logger Your custom logger
 */
export const setLogger = (logger: BaseLogger): void => {
  outboxLogger = logger;
};

/**
 * Gets the logger instance to log messages
 * @returns The default or custom defined logger
 */
export const logger = (): BaseLogger => {
  return outboxLogger;
};

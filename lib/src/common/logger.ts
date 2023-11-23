/* eslint-disable @typescript-eslint/no-empty-function */
import pino, { BaseLogger } from 'pino';

export type TransactionalLogger = BaseLogger;

export const getDefaultLogger = (name = 'default'): TransactionalLogger =>
  pino({ name, timestamp: pino.stdTimeFunctions.isoTime });

/**
 * Disable the logger.
 */
export const getDisabledLogger = (): TransactionalLogger => ({
  fatal: () => {},
  error: () => {},
  warn: () => {},
  info: () => {},
  debug: () => {},
  trace: () => {},
  silent: () => {},
  level: 'silent',
});

/**
 * Writes all logs to an array that is returned from this call.
 * @param context Add this to every log entry
 * @returns The logger instance and the array of in-memory logs
 */
export const getInMemoryLogger = (
  context: string,
): [logger: TransactionalLogger, logs: InMemoryLogEntry[]] => {
  const logs: InMemoryLogEntry[] = [];
  const l =
    (type: string) =>
    (...args: unknown[]) => {
      logs.push({ type, args, date: new Date().toISOString(), context });
    };
  const logger = {
    fatal: l('fatal'),
    error: l('error'),
    warn: l('warn'),
    info: l('info'),
    debug: l('debug'),
    trace: l('trace'),
    silent: l('silent'),
    level: 'trace',
  };
  return [logger, logs];
};

export interface InMemoryLogEntry {
  context: string;
  type: string;
  date: string;
  args: unknown[];
}

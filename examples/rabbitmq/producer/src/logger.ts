import { TransactionalLogger } from 'pg-transactional-outbox';
import pino, { LoggerOptions } from 'pino';

const options: LoggerOptions = {
  level: process.env.LOG_LEVEL ?? 'info',
  messageKey: 'msg',
};

if (process.env.NODE_ENV === 'development') {
  options.formatters = {
    log: (value: Record<string, unknown>) => {
      let payload = value;
      if (
        'payload' in value &&
        typeof value.payload === 'object' &&
        value.payload
      ) {
        payload = value.payload as Record<string, unknown>;
      }
      if ('id' in payload && 'title' in payload && 'description' in payload) {
        return {
          context: `Movie '${payload.title}' with id '${payload.id}'`,
          err: value.err,
        };
      }
      return value;
    },
  };
  options.transport = {
    target: 'pino-pretty',
    options: {
      colorize: true,
    },
  };
}

export const getLogger = (): TransactionalLogger => pino(options);

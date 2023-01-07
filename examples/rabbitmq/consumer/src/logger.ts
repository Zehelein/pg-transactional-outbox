/* eslint-disable @typescript-eslint/no-var-requires */
import pino, { LoggerOptions } from 'pino';

const options: LoggerOptions = {
  level: process.env.LOG_LEVEL ?? 'info',
  messageKey: 'msg',
};

if (process.env.NODE_ENV === 'development') {
  options.formatters = {
    log: (value: Record<string, unknown>) => {
      if (
        'id' in value &&
        'aggregateType' in value &&
        'aggregateId' in value &&
        'eventType' in value
      ) {
        return {
          context: `Message for ${value.aggregateType}.${value.eventType}.${value.aggregateId} has ID ${value.id}`,
          err: value.err,
        };
      }
      if ('id' in value && 'title' in value && 'description' in value) {
        return {
          context: `Movie ${value.title} with id ${value.id}`,
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

export const logger = pino(options);

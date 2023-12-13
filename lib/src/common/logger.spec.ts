import {
  getDefaultLogger,
  getDisabledLogger,
  getInMemoryLogger,
  InMemoryLogEntry,
} from './logger';

describe('logger', () => {
  it('getDefaultLogger should create a pino logger', () => {
    const logger: any = getDefaultLogger('unit-test');
    expect(logger.onChild.name).toBe('noop'); // initialized without a child logger
  });

  it('getDefaultLogger with default name should create a pino logger', () => {
    const logger: any = getDefaultLogger();
    expect(logger.onChild.name).toBe('noop'); // initialized without a child logger
  });

  it('getInMemoryLogger should write all logs to an array', () => {
    const context = 'test';
    const [logger, logs] = getInMemoryLogger(context);
    const message = 'test message';
    logger.fatal(message);
    logger.error(message);
    logger.warn(message);
    logger.info(message);
    logger.debug(message);
    logger.trace(message);
    logger.silent(message);
    expect(logs).toEqual([
      { context, type: 'fatal', date: expect.any(String), args: [message] },
      { context, type: 'error', date: expect.any(String), args: [message] },
      { context, type: 'warn', date: expect.any(String), args: [message] },
      { context, type: 'info', date: expect.any(String), args: [message] },
      { context, type: 'debug', date: expect.any(String), args: [message] },
      { context, type: 'trace', date: expect.any(String), args: [message] },
      { context, type: 'silent', date: expect.any(String), args: [message] },
    ] as InMemoryLogEntry[]);
  });

  it('should create a new logger instance that does not log anything', () => {
    // Arrange
    const logger = getDisabledLogger();

    // Assert
    expect(logger.fatal).toBeInstanceOf(Function);
    expect(logger.error).toBeInstanceOf(Function);
    expect(logger.warn).toBeInstanceOf(Function);
    expect(logger.info).toBeInstanceOf(Function);
    expect(logger.debug).toBeInstanceOf(Function);
    expect(logger.trace).toBeInstanceOf(Function);
    expect(logger.silent).toBeInstanceOf(Function);
    expect(logger.level).toBe('silent');

    expect(logger.fatal('test')).toBeUndefined();
    expect(logger.error('test')).toBeUndefined();
    expect(logger.warn('test')).toBeUndefined();
    expect(logger.info('test')).toBeUndefined();
    expect(logger.debug('test')).toBeUndefined();
    expect(logger.trace('test')).toBeUndefined();
    expect(logger.silent('test')).toBeUndefined();

    // Functions do not take an input parameter in the disabled logger
    const regex = /\(\) => \{.*?/gm;
    expect(logger.fatal.toString()).toMatch(regex);
    expect(logger.error.toString()).toMatch(regex);
    expect(logger.warn.toString()).toMatch(regex);
    expect(logger.info.toString()).toMatch(regex);
    expect(logger.debug.toString()).toMatch(regex);
    expect(logger.trace.toString()).toMatch(regex);
    expect(logger.silent.toString()).toMatch(regex);
  });
});

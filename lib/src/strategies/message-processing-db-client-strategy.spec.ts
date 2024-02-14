import { ClientBase, Pool } from 'pg';
import { ListenerConfig } from '../common/listener-config';
import { getDisabledLogger, getInMemoryLogger } from '../common/logger';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { defaultMessageProcessingDbClientStrategy } from './message-processing-db-client-strategy';

let errorHandler: (err: Error) => void;
let pool: Pool;
jest.mock('pg', () => {
  return {
    Pool: jest.fn().mockImplementation(() => pool),
  };
});

describe('defaultMessageProcessingDbClientStrategy', () => {
  beforeEach(() => {
    pool = {
      on: jest.fn((event: 'error', callback: (err: Error) => void) => {
        errorHandler = callback;
      }),
      connect: jest.fn().mockResolvedValue({
        on: jest.fn(),
        query: jest.fn(),
        release: jest.fn(),
        listeners: jest.fn().mockReturnValue([]),
        escapeLiteral: (p: string) => p,
      }),
      removeAllListeners: jest.fn(),
      end: jest.fn(),
    } as unknown as Pool;
  });

  it('should return a DB client', async () => {
    const strategy = defaultMessageProcessingDbClientStrategy(
      {} as ListenerConfig,
      getDisabledLogger(),
    );
    const client = await strategy.getClient({
      id: '1',
    } as StoredTransactionalMessage);
    expect((client as ClientBase).escapeLiteral('x')).toBe('x');
  });

  it('should log an error when the client encounters an error', async () => {
    // Arrange
    const [logger, logs] = getInMemoryLogger('test');
    const strategy = defaultMessageProcessingDbClientStrategy(
      {} as ListenerConfig,
      logger,
    );
    const error = new Error('unit test');

    // Act
    await strategy.getClient({ id: '1' } as StoredTransactionalMessage);

    // Assert
    expect(errorHandler).toBeDefined();
    errorHandler(error);
    expect(logs[0].args[0]).toBe(error);
  });

  it('should remove all listeners and end the pool on shutdown', async () => {
    const strategy = defaultMessageProcessingDbClientStrategy(
      {} as ListenerConfig,
      getDisabledLogger(),
    );
    await strategy.shutdown();

    expect(pool.removeAllListeners).toHaveBeenCalled();
    expect(pool.end).toHaveBeenCalled();
  });

  it('should log an error when pool shutdown encounters an error', async () => {
    // Arrange
    const error = new Error('Test error');
    pool.end = jest.fn().mockRejectedValue(error);
    const [logger, logs] = getInMemoryLogger('test');
    const strategy = defaultMessageProcessingDbClientStrategy(
      {} as ListenerConfig,
      logger,
    );

    // Act
    await strategy.shutdown();

    // Assert
    const log = logs.filter(
      (log) => log.args[1] === 'Message processing pool shutdown error',
    );
    expect(log).toHaveLength(1);
  });
});

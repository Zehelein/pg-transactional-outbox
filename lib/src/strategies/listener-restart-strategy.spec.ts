import { Pool } from 'pg';
import { BaseLogger } from 'pino';
import { MessageError, OutboxMessage } from '../../dist';
import { TransactionalOutboxInboxConfig } from '../replication/config';
import {
  defaultListenerAndSlotRestartStrategy,
  defaultListenerRestartStrategy,
} from './listener-restart-strategy';

const query = jest.fn();
const end = jest.fn();
jest.mock('pg', () => {
  return {
    Pool: jest.fn(() => ({
      query,
      end,
    })),
  };
});

describe.each([
  defaultListenerRestartStrategy,
  defaultListenerAndSlotRestartStrategy,
])('Should return the same %p', (strategyFunction) => {
  const config = {
    settings: {
      restartDelay: 123,
      restartDelaySlotInUse: 1234,
    },
  } as TransactionalOutboxInboxConfig;
  let logger: BaseLogger;

  beforeEach(() => {
    logger = {
      trace: jest.fn(),
      error: jest.fn(),
    } as unknown as BaseLogger;
    jest.restoreAllMocks();
  });

  it('should return restartDelaySlotInUse for PostgreSQL replication slot in use error', async () => {
    // Arrange
    const error = {
      code: '55006',
      routine: 'ReplicationSlotAcquire',
    } as unknown as Error;
    const strategy = strategyFunction(config);

    // Act
    const result = await strategy(error, logger, 'inbox');

    // Assert
    expect(result).toBe(config.settings.restartDelaySlotInUse);
    expect(logger.trace).toHaveBeenCalledWith(
      error,
      'The replication slot for the inbox listener is currently in use.',
    );
    expect(logger.trace).not.toHaveBeenCalledWith(
      expect.any(Error),
      'Failed to create the replication slot for the inbox which does not exist.',
    );
  });

  it('should return restartDelay for MessageErrors but not log the error', async () => {
    // Arrange
    const error = new MessageError(
      'some_other_code',
      {} as OutboxMessage,
      new Error('test'),
    );
    const strategy = strategyFunction(config);

    // Act
    const result = await strategy(error, logger, 'outbox');

    // Assert
    expect(result).toBe(config.settings.restartDelay);
    expect(logger.error).not.toHaveBeenCalledWith(
      error,
      'Transactional outbox listener error',
    );
    expect(logger.trace).not.toHaveBeenCalledWith(
      expect.any(Error),
      'Failed to create the replication slot for the inbox which does not exist.',
    );
  });

  it('should use default delays if they are not set', async () => {
    // Arrange
    const config = {
      settings: {},
    } as TransactionalOutboxInboxConfig;
    const strategy = strategyFunction(config);

    // Act + Assert
    let error = {
      code: '55006',
      routine: 'ReplicationSlotAcquire',
    } as unknown as Error;
    let result = await strategy(error, logger, 'outbox');
    expect(result).toBe(10_000);

    error = {
      code: '42704',
      routine: 'ReplicationSlotAcquire',
    } as unknown as Error;
    result = await strategy(error, logger, 'outbox');
    expect(result).toBe(250);

    error = {
      code: '12345',
      routine: 'ReplicationSlotAcquire',
    } as unknown as Error;
    result = await strategy(error, logger, 'outbox');
    expect(result).toBe(250);

    error = new MessageError(
      'some_other_code',
      {} as OutboxMessage,
      new Error('test'),
    );
    result = await strategy(error, logger, 'outbox');
    expect(result).toBe(250);

    error = new Error('other error');
    result = await strategy(error, logger, 'outbox');
    expect(result).toBe(250);
  });

  it('should return restartDelay for other errors', async () => {
    // Arrange
    const error = new Error('some_other_code');
    const strategy = strategyFunction(config);

    // Act
    const result = await strategy(error, logger, 'outbox');

    // Assert
    expect(result).toBe(config.settings.restartDelay);
    expect(logger.error).toHaveBeenCalledWith(
      error,
      'Transactional outbox listener error',
    );
    expect(logger.trace).not.toHaveBeenCalledWith(
      expect.any(Error),
      'Failed to create the replication slot for the inbox which does not exist.',
    );
  });

  it('should create a new replication slot if not found', async () => {
    // Arrange
    const error = {
      code: '42704',
      routine: 'ReplicationSlotAcquire',
    } as unknown as Error;
    const strategy = strategyFunction(config);

    // Act
    const result = await strategy(error, logger, 'inbox');

    // Assert
    expect(result).toBe(config.settings.restartDelay);
    if (strategyFunction.name === defaultListenerAndSlotRestartStrategy.name) {
      /* eslint-disable jest/no-conditional-expect */
      expect(Pool).toHaveBeenCalledWith(config.pgReplicationConfig);
      expect(query).toHaveBeenCalled();
      expect(end).toHaveBeenCalled();
      expect(logger.trace).not.toHaveBeenCalledWith(
        expect.any(Error),
        'Failed to create the replication slot for the inbox which does not exist.',
      );
    }
    /* eslint-enable jest/no-conditional-expect */
  });

  it('should trace the error when creating a new replication slot fails', async () => {
    // Arrange
    const error = {
      code: '42704',
      routine: 'ReplicationSlotAcquire',
    } as unknown as Error;
    const strategy = strategyFunction(config);
    query.mockRejectedValueOnce(new Error('Query error'));

    // Act
    const result = await strategy(error, logger, 'inbox');

    // Assert
    expect(result).toBe(config.settings.restartDelay);
    if (strategyFunction.name === defaultListenerAndSlotRestartStrategy.name) {
      /* eslint-disable jest/no-conditional-expect */
      expect(Pool).toHaveBeenCalledWith(config.pgReplicationConfig);
      expect(query).toHaveBeenCalled();
      expect(end).toHaveBeenCalled();
      expect(result).toBe(config.settings.restartDelay);
      expect(logger.trace).toHaveBeenCalledWith(
        expect.any(Error),
        'Failed to create the replication slot for the inbox which does not exist.',
      );
    }
    /* eslint-enable jest/no-conditional-expect */
  });
});

import { DatabaseError, Pool } from 'pg';
import { BaseLogger } from 'pino';
import { ErrorCode, ExtendedError, MessageError } from '../common/error';
import { TransactionalMessage } from '../message/message';
import { ReplicationConfig } from '../replication/config';
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

const createPgError = (
  code: string,
): DatabaseError & {
  errorCode: ErrorCode;
} => {
  const error = new Error('PostgreSQL error') as DatabaseError & {
    errorCode: ErrorCode;
  };
  error.code = code;
  error.errorCode = 'DB_ERROR';
  error.routine = 'ReplicationSlotAcquire';
  return error;
};

describe.each([
  defaultListenerRestartStrategy,
  defaultListenerAndSlotRestartStrategy,
])('Should return the same %p', (strategyFunction) => {
  const config = {
    settings: {
      restartDelay: 123,
      restartDelaySlotInUse: 1234,
    },
  } as ReplicationConfig;
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
    const error = createPgError('55006');
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
      'MESSAGE_HANDLING_FAILED',
      {} as TransactionalMessage,
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
    } as ReplicationConfig;
    const strategy = strategyFunction(config);

    // Act + Assert
    let pgError = createPgError('55006');
    let result = await strategy(pgError, logger, 'outbox');
    expect(result).toBe(10_000);

    pgError = createPgError('42704');
    result = await strategy(pgError, logger, 'outbox');
    expect(result).toBe(250);

    pgError = createPgError('12345');
    result = await strategy(pgError, logger, 'outbox');
    expect(result).toBe(250);

    const messageError = new MessageError(
      'some_other_code',
      'MESSAGE_HANDLING_FAILED',
      {} as TransactionalMessage,
      new Error('test'),
    );
    result = await strategy(messageError, logger, 'outbox');
    expect(result).toBe(250);

    const error = new Error('other error') as ExtendedError;
    error.errorCode = 'MESSAGE_STORAGE_FAILED';
    result = await strategy(error, logger, 'outbox');
    expect(result).toBe(250);
  });

  it('should return restartDelay for other errors', async () => {
    // Arrange
    const error = new Error('some_other_code') as ExtendedError;
    error.errorCode = 'LISTENER_STOPPED';
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
    const error = createPgError('42704');
    const strategy = strategyFunction(config);

    // Act
    const result = await strategy(error, logger, 'inbox');

    // Assert
    expect(result).toBe(config.settings.restartDelay);
    if (strategyFunction.name === defaultListenerAndSlotRestartStrategy.name) {
      /* eslint-disable jest/no-conditional-expect */
      expect(Pool).toHaveBeenCalledWith(config.dbListenerConfig);
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
    const error = createPgError('42704');
    const strategy = strategyFunction(config);
    query.mockRejectedValueOnce(new Error('Query error'));

    // Act
    const result = await strategy(error, logger, 'inbox');

    // Assert
    expect(result).toBe(config.settings.restartDelay);
    if (strategyFunction.name === defaultListenerAndSlotRestartStrategy.name) {
      /* eslint-disable jest/no-conditional-expect */
      expect(Pool).toHaveBeenCalledWith(config.dbListenerConfig);
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

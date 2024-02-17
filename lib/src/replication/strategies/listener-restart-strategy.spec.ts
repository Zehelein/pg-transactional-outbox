import { DatabaseError, Pool } from 'pg';
import { BaseLogger } from 'pino';
import { ErrorCode, ExtendedError, MessageError } from '../../common/error';
import { TransactionalMessage } from '../../message/transactional-message';
import { FullReplicationListenerConfig } from '../config';
import {
  defaultReplicationListenerAndSlotRestartStrategy,
  defaultReplicationListenerRestartStrategy,
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
  defaultReplicationListenerRestartStrategy,
  defaultReplicationListenerAndSlotRestartStrategy,
])('Should return the same %p', (strategyFunction) => {
  const config = {
    settings: {
      restartDelayInMs: 123,
      restartDelaySlotInUseInMs: 1234,
    },
  } as FullReplicationListenerConfig;
  let logger: BaseLogger;

  beforeEach(() => {
    logger = {
      trace: jest.fn(),
      error: jest.fn(),
    } as unknown as BaseLogger;
    query.mockReset();
    end.mockReset();
  });

  it('should return restartDelaySlotInUseInMs for PostgreSQL replication slot in use error', async () => {
    // Arrange
    const error = createPgError('55006');
    const strategy = strategyFunction(config);

    // Act
    const result = await strategy(error, logger, 'inbox');

    // Assert
    expect(result).toBe(config.settings.restartDelaySlotInUseInMs);
    expect(logger.trace).toHaveBeenCalledWith(
      error,
      'The replication slot for the inbox listener is currently in use.',
    );
    expect(logger.trace).not.toHaveBeenCalledWith(
      expect.any(Error),
      'Failed to create the replication slot for the inbox which does not exist.',
    );
  });

  it('should return restartDelayInMs for MessageErrors but not log the error', async () => {
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
    expect(result).toBe(config.settings.restartDelayInMs);
    expect(logger.error).not.toHaveBeenCalledWith(
      error,
      'Transactional outbox listener error',
    );
    expect(logger.trace).not.toHaveBeenCalledWith(
      expect.any(Error),
      'Failed to create the replication slot for the inbox which does not exist.',
    );
  });

  it('should return restartDelayInMs for other errors', async () => {
    // Arrange
    const error = new Error('some_other_code') as ExtendedError;
    error.errorCode = 'LISTENER_STOPPED';
    const strategy = strategyFunction(config);

    // Act
    const result = await strategy(error, logger, 'outbox');

    // Assert
    expect(result).toBe(config.settings.restartDelayInMs);
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
    expect(result).toBe(config.settings.restartDelayInMs);
    if (
      strategyFunction.name ===
      defaultReplicationListenerAndSlotRestartStrategy.name
    ) {
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
    expect(result).toBe(config.settings.restartDelayInMs);
    if (
      strategyFunction.name ===
      defaultReplicationListenerAndSlotRestartStrategy.name
    ) {
      /* eslint-disable jest/no-conditional-expect */
      expect(Pool).toHaveBeenCalledWith(config.dbListenerConfig);
      expect(query).toHaveBeenCalled();
      expect(end).toHaveBeenCalled();
      expect(result).toBe(config.settings.restartDelayInMs);
      expect(logger.trace).toHaveBeenCalledWith(
        expect.any(Error),
        'Failed to create the replication slot for the inbox which does not exist.',
      );
    }
    /* eslint-enable jest/no-conditional-expect */
  });
});

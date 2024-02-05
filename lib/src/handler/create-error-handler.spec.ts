import { PoolClient } from 'pg';
import { ListenerConfig } from '../common/base-config';
import { TransactionalOutboxInboxError } from '../common/error';
import { getDisabledLogger } from '../common/logger';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { createErrorHandler } from './create-error-handler';
import { TransactionalMessageHandler } from './transactional-message-handler';

const message: StoredTransactionalMessage = {
  id: '6c1f3e2b-76f0-41aa-86b2-bae105eca0ac',
  aggregateId: '123',
  aggregateType: 'movie',
  messageType: 'update',
  concurrency: 'sequential',
  createdAt: new Date().toISOString(),
  payload: { test: true },
  lockedUntil: new Date().toISOString(),
  processedAt: null,
  startedAttempts: 1,
  finishedAttempts: 0,
};

interface Result {
  rowCount: number;
  rows: any[];
}

interface ClientArgs {
  started_attempts?: number;
  finished_attempts?: number;
  increaseMessageFinishedAttemptsResult?: Result;
}

function getClient({
  started_attempts = 1,
  finished_attempts = 0,
  increaseMessageFinishedAttemptsResult,
}: ClientArgs) {
  const client = {
    increaseMessageFinishedAttempts: 0,
    query(sql: string, params: [any]) {
      if (
        sql.includes(
          'UPDATE test_schema.test_table SET finished_attempts = finished_attempts + 1 WHERE id = $1;',
        )
      ) {
        client.increaseMessageFinishedAttempts++;
        return (
          increaseMessageFinishedAttemptsResult ?? {
            rowCount: 1,
            rows: [
              {
                started_attempts,
                finished_attempts,
                processed_at: null,
              },
            ],
          }
        );
      } else {
        return { rowCount: 0, rows: [] }; // BEGIN, COMMIT, ...
      }
    },
    release() {},
  } as unknown as PoolClient & {
    increaseMessageFinishedAttempts: number;
  };
  return client;
}

describe('createErrorHandler', () => {
  it.each([true, false])(
    'Should handle an error and increase the finished attempts',
    async (shouldRetry: boolean) => {
      // Arrange
      const client = getClient({});
      const strategies = {
        messageProcessingTransactionLevelStrategy: jest
          .fn()
          .mockReturnValue(undefined),
        messageProcessingDbClientStrategy: {
          getClient: async () => client,
          shutdown: jest.fn(),
        },
        poisonousMessageRetryStrategy: jest.fn().mockReturnValue(false),
        messageRetryStrategy: jest.fn().mockReturnValue(shouldRetry),
        messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(1000),
      };
      const config: ListenerConfig = {
        outboxOrInbox: 'inbox',
        dbListenerConfig: {},
        settings: {
          dbSchema: 'test_schema',
          dbTable: 'test_table',
          enablePoisonousMessageProtection: true,
          enableMaxAttemptsProtection: true,
        },
      };
      const handler = { handle: jest.fn(), handleError: jest.fn() };
      const errorHandler = createErrorHandler(
        handler,
        strategies,
        config,
        getDisabledLogger(),
      );
      const mockMessage = {
        ...message,
      };
      const error = new TransactionalOutboxInboxError(
        'test',
        'MESSAGE_HANDLING_FAILED',
      );

      // Act
      const retryAnswer = await errorHandler(mockMessage, error);

      // Assert
      expect(handler.handleError).toHaveBeenCalledWith(
        error,
        mockMessage,
        client,
        shouldRetry,
      );
      expect(client.increaseMessageFinishedAttempts).toBe(1);
      expect(retryAnswer).toBe(shouldRetry);
      expect(mockMessage).toStrictEqual({ ...message, finishedAttempts: 1 });
      expect(
        strategies.messageProcessingTransactionLevelStrategy,
      ).toHaveBeenCalledWith(mockMessage);
      expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
      expect(strategies.messageRetryStrategy).toHaveBeenCalled();
    },
  );

  it('Should still increase the finished attempts even if the handler threw an error', async () => {
    // Arrange
    const client = getClient({});
    const strategies = {
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(undefined),
      messageProcessingDbClientStrategy: {
        getClient: async () => client,
        shutdown: jest.fn(),
      },
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(false),
      messageRetryStrategy: jest.fn().mockReturnValue(true),
      messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(1000),
    };
    const config: ListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        enablePoisonousMessageProtection: true,
        enableMaxAttemptsProtection: true,
      },
    };
    const handler = {
      handle: jest.fn(() => {
        throw new Error('error handler error');
      }),
      handleError: jest.fn(),
    };
    const errorHandler = createErrorHandler(
      handler,
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const error = new TransactionalOutboxInboxError(
      'test',
      'MESSAGE_HANDLING_FAILED',
    );

    // Act
    const retryAnswer = await errorHandler(mockMessage, error);

    // Assert
    expect(handler.handleError).toHaveBeenCalledWith(
      error,
      mockMessage,
      client,
      true,
    );
    expect(client.increaseMessageFinishedAttempts).toBe(1);
    expect(retryAnswer).toBe(true);
    expect(mockMessage).toStrictEqual({ ...message, finishedAttempts: 1 });
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).toHaveBeenCalled();
  });

  it('Should not retry a message when the handler threw an error and the best-effort finished attempts increase as well', async () => {
    // Arrange
    const client = getClient({});
    client.query = jest
      .fn()
      .mockReturnValueOnce({ rows: 0 }) // begin
      .mockReturnValueOnce({ rows: 0 }) // rollback
      .mockReturnValueOnce({ rows: 0 }) // begin
      .mockRejectedValueOnce(
        new Error('Best effort finished attempts increment error'),
      );
    const strategies = {
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(undefined),
      messageProcessingDbClientStrategy: {
        getClient: async () => client,
        shutdown: jest.fn(),
      },
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(false),
      messageRetryStrategy: jest.fn().mockReturnValue(true),
      messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(1000),
    };
    const config: ListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        enablePoisonousMessageProtection: true,
        enableMaxAttemptsProtection: true,
      },
    };
    const handler = {
      handle: jest.fn(),
      handleError: jest.fn(() => {
        throw new Error('error handler error');
      }),
    };
    const errorHandler = createErrorHandler(
      handler,
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const error = new TransactionalOutboxInboxError(
      'test',
      'MESSAGE_HANDLING_FAILED',
    );

    // Act
    const retryAnswer = await errorHandler(mockMessage, error);

    // Assert
    expect(handler.handleError).toHaveBeenCalledWith(
      error,
      mockMessage,
      client,
      true,
    );
    expect(retryAnswer).toBe(false);
    expect(mockMessage).toStrictEqual({ ...message, finishedAttempts: 1 });
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).toHaveBeenCalled();
  });

  it('Should increase the finished attempts even if no handler is found (anymore)', async () => {
    // Arrange
    const client = getClient({});
    const strategies = {
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(undefined),
      messageProcessingDbClientStrategy: {
        getClient: async () => client,
        shutdown: jest.fn(),
      },
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(false),
      messageRetryStrategy: jest.fn().mockReturnValue(true),
      messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(1000),
    };
    const config: ListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        enablePoisonousMessageProtection: true,
        enableMaxAttemptsProtection: true,
      },
    };
    const handler: TransactionalMessageHandler = {
      aggregateType: 'not-mapped',
      messageType: 'not-mapped',
      handle: jest.fn(),
    };
    const errorHandler = createErrorHandler(
      [handler],
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const error = new TransactionalOutboxInboxError(
      'test',
      'MESSAGE_HANDLING_FAILED',
    );

    // Act
    const retryAnswer = await errorHandler(mockMessage, error);

    // Assert
    expect(client.increaseMessageFinishedAttempts).toBe(1);
    expect(retryAnswer).toBe(true);
    expect(mockMessage).toStrictEqual({ ...message, finishedAttempts: 1 });
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).toHaveBeenCalled();
  });

  it('Should increase the finished attempts even if no error handling method is defined', async () => {
    // Arrange
    const client = getClient({});
    const strategies = {
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(undefined),
      messageProcessingDbClientStrategy: {
        getClient: async () => client,
        shutdown: jest.fn(),
      },
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(false),
      messageRetryStrategy: jest.fn().mockReturnValue(true),
      messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(1000),
    };
    const config: ListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        enablePoisonousMessageProtection: true,
        enableMaxAttemptsProtection: true,
      },
    };
    const handler = { handle: jest.fn() };
    const errorHandler = createErrorHandler(
      handler,
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const error = new TransactionalOutboxInboxError(
      'test',
      'MESSAGE_HANDLING_FAILED',
    );

    // Act
    const retryAnswer = await errorHandler(mockMessage, error);

    // Assert
    expect(client.increaseMessageFinishedAttempts).toBe(1);
    expect(retryAnswer).toBe(true);
    expect(mockMessage).toStrictEqual({ ...message, finishedAttempts: 1 });
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).toHaveBeenCalled();
  });
});

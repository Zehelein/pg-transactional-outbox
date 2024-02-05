import { EventEmitter } from 'events';
import { PoolClient } from 'pg';
import { ListenerConfig } from '../common/base-config';
import { getDisabledLogger } from '../common/logger';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { defaultMessageRetryStrategy } from '../strategies/message-retry-strategy';
import { createMessageHandler } from './create-message-handler';
import { TransactionalMessageHandler } from './transactional-message-handler';

const message: StoredTransactionalMessage = {
  id: '6c1f3e2b-76f0-41aa-86b2-bae105eca0ac',
  aggregateId: '123',
  aggregateType: 'movie',
  messageType: 'update',
  createdAt: new Date().toISOString(),
  payload: { test: true },
  processedAt: null,
  startedAttempts: 0,
  finishedAttempts: 0,
};

interface Result {
  rowCount: number;
  rows: any[];
}

interface ClientArgs {
  started_attempts?: number;
  finished_attempts?: number;
  startedAttemptsIncrementResult?: Result;
  initiateMessageProcessingResult?: Result;
}

function getClient({
  started_attempts = 1,
  finished_attempts = 0,
  startedAttemptsIncrementResult,
  initiateMessageProcessingResult,
}: ClientArgs) {
  const client = {
    startedAttemptsIncrement: 0,
    initiateMessageProcessing: 0,
    markMessageCompleted: 0,
    query(sql: string, params: [any]) {
      if (
        sql.includes(
          'UPDATE test_schema.test_table SET started_attempts = started_attempts + 1 WHERE id IN',
        )
      ) {
        client.startedAttemptsIncrement++;
        return (
          startedAttemptsIncrementResult ?? {
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
      } else if (
        sql.includes(
          'SELECT started_attempts, finished_attempts, processed_at FROM test_schema.test_table WHERE id = $1 FOR UPDATE NOWAIT;',
        )
      ) {
        client.initiateMessageProcessing++;
        return {
          rowCount: 1,
          rows: initiateMessageProcessingResult ?? [
            {
              started_attempts,
              finished_attempts,
              processed_at: null,
            },
          ],
        };
      } else if (
        sql.includes(
          'UPDATE test_schema.test_table SET processed_at = $1, finished_attempts = finished_attempts + 1 WHERE id = $2',
        )
      ) {
        client.markMessageCompleted++;
        return { rowCount: 0, rows: [] };
      } else {
        return { rowCount: 0, rows: [] }; // BEGIN, COMMIT, ...
      }
    },
    release() {},
  } as unknown as PoolClient & {
    startedAttemptsIncrement: number;
    initiateMessageProcessing: number;
    markMessageCompleted: number;
  };
  return client;
}

describe('createMessageHandler', () => {
  it('Should handle a message and mark it as processed', async () => {
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
      messageRetryStrategy: jest.fn().mockReturnValue(false),
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
    const messageHandler = createMessageHandler(
      handler,
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const cancellation = new EventEmitter();

    // Act
    await messageHandler(mockMessage, cancellation);

    // Assert
    expect(handler.handle).toHaveBeenCalledWith(mockMessage, client);
    expect(client.startedAttemptsIncrement).toBe(1);
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.markMessageCompleted).toBe(1);
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).not.toHaveBeenCalled();
  });

  it('When poisonous checks are disabled the attempts increase and poisonous retry strategy should not be called', async () => {
    // Arrange
    const client = getClient({ started_attempts: 2 });
    const strategies = {
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(undefined),
      messageProcessingDbClientStrategy: {
        getClient: async () => client,
        shutdown: jest.fn(),
      },
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(true),
      messageRetryStrategy: jest.fn().mockReturnValue(false),
      messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(1000),
    };
    const config: ListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        enablePoisonousMessageProtection: false,
        enableMaxAttemptsProtection: true,
      },
    };
    const handler = { handle: jest.fn() };
    const messageHandler = createMessageHandler(
      handler,
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const cancellation = new EventEmitter();

    // Act
    await messageHandler(mockMessage, cancellation);

    // Assert
    expect(handler.handle).toHaveBeenCalledWith(mockMessage, client);
    expect(client.startedAttemptsIncrement).toBe(0);
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.markMessageCompleted).toBe(1);
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).not.toHaveBeenCalled();
  });

  it('When the started attempts increment failed do not process the message', async () => {
    // Arrange
    const client = getClient({
      startedAttemptsIncrementResult: { rowCount: 0, rows: [] },
    });
    const strategies = {
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(undefined),
      messageProcessingDbClientStrategy: {
        getClient: async () => client,
        shutdown: jest.fn(),
      },
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(false),
      messageRetryStrategy: jest.fn().mockReturnValue(false),
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
    const messageHandler = createMessageHandler(
      handler,
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
      startedAttempts: 2,
      finishedAttempts: 0,
    };
    const cancellation = new EventEmitter();

    // Act
    await messageHandler(mockMessage, cancellation);

    // Assert
    expect(handler.handle).not.toHaveBeenCalled();
    expect(client.startedAttemptsIncrement).toBe(1);
    expect(client.initiateMessageProcessing).toBe(0);
    expect(client.markMessageCompleted).toBe(0);
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).not.toHaveBeenCalled();
  });

  it('When the start to finished diff is two and the poisonous retry strategy returns falls, the handler is not called', async () => {
    // Arrange
    const client = getClient({ started_attempts: 2 });
    const strategies = {
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(undefined),
      messageProcessingDbClientStrategy: {
        getClient: async () => client,
        shutdown: jest.fn(),
      },
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(true),
      messageRetryStrategy: jest.fn().mockReturnValue(true),
      messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(1000),
    };
    const config: ListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        enablePoisonousMessageProtection: false,
        enableMaxAttemptsProtection: true,
      },
    };
    const handler = { handle: jest.fn() };
    const messageHandler = createMessageHandler(
      handler,
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const cancellation = new EventEmitter();

    // Act
    await messageHandler(mockMessage, cancellation);

    // Assert
    expect(handler.handle).toHaveBeenCalledWith(mockMessage, client);
    expect(client.startedAttemptsIncrement).toBe(0);
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.markMessageCompleted).toBe(1);
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).not.toHaveBeenCalled();
  });

  it('Should handle a message and mark it as processed when a potential poisonous message should be retried', async () => {
    // Arrange
    const client = getClient({ started_attempts: 100 });
    const strategies = {
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(undefined),
      messageProcessingDbClientStrategy: {
        getClient: async () => client,
        shutdown: jest.fn(),
      },
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(true),
      messageRetryStrategy: jest.fn().mockReturnValue(false),
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
    const messageHandler = createMessageHandler(
      handler,
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const cancellation = new EventEmitter();

    // Act
    await messageHandler(mockMessage, cancellation);

    // Assert
    expect(handler.handle).toHaveBeenCalledWith(mockMessage, client);
    expect(client.startedAttemptsIncrement).toBe(1);
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.markMessageCompleted).toBe(1);
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).not.toHaveBeenCalled();
  });

  it('When no handler is found it should skip most logic and just mark message as completed', async () => {
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
      messageRetryStrategy: jest.fn().mockReturnValue(false),
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
      handle: jest.fn(),
      aggregateType: 'no-match',
      messageType: 'no-match',
    };
    const messageHandler = createMessageHandler(
      [handler],
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const cancellation = new EventEmitter();

    // Act
    await messageHandler(mockMessage, cancellation);

    // Assert
    expect(handler.handle).not.toHaveBeenCalled();
    expect(client.startedAttemptsIncrement).toBe(0);
    expect(client.initiateMessageProcessing).toBe(0);
    expect(client.markMessageCompleted).toBe(1);
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).not.toHaveBeenCalled();
  });

  it('Should double check that a message is not processed if max attempts are exceeded', async () => {
    // Arrange
    const client = getClient({ finished_attempts: 6 });
    const config: ListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        enablePoisonousMessageProtection: true,
        enableMaxAttemptsProtection: true,
        maxAttempts: 5,
      },
    };
    const strategies = {
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(undefined),
      messageProcessingDbClientStrategy: {
        getClient: async () => client,
        shutdown: jest.fn(),
      },
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(true),
      messageRetryStrategy: defaultMessageRetryStrategy(config),
      messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(1000),
    };

    const handler = { handle: jest.fn() };
    const messageHandler = createMessageHandler(
      handler,
      strategies,
      config,
      getDisabledLogger(),
    );
    const mockMessage = {
      ...message,
    };
    const cancellation = new EventEmitter();

    // Act
    await messageHandler(mockMessage, cancellation);

    // Assert
    expect(handler.handle).not.toHaveBeenCalled();
    expect(client.startedAttemptsIncrement).toBe(1);
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.markMessageCompleted).toBe(0);
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
  });
});

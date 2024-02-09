import { EventEmitter } from 'events';
import { PoolClient } from 'pg';
import { getDisabledLogger } from '../common/logger';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { ReplicationConfig } from '../replication/config';
import { defaultMessageRetryStrategy } from '../strategies/message-retry-strategy';
import { ListenerType, createMessageHandler } from './create-message-handler';
import { TransactionalMessageHandler } from './transactional-message-handler';

const message: StoredTransactionalMessage = {
  id: '6c1f3e2b-76f0-41aa-86b2-bae105eca0ac',
  aggregateId: '123',
  aggregateType: 'movie',
  messageType: 'update',
  createdAt: new Date().toISOString(),
  payload: { test: true },
  concurrency: 'sequential',
  lockedUntil: new Date(Date.now() + 5 * 60 * 1000).toISOString(),
  processedAt: null,
  abandonedAt: null,
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
    markMessageAbandoned: 0,
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
                abandoned_at: null,
              },
            ],
          }
        );
      } else if (
        sql.includes(
          'SELECT started_attempts, finished_attempts, processed_at, abandoned_at, locked_until FROM test_schema.test_table WHERE id = $1 FOR NO KEY UPDATE NOWAIT;',
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
          'UPDATE test_schema.test_table SET processed_at = NOW(), finished_attempts = finished_attempts + 1 WHERE id = $1',
        )
      ) {
        client.markMessageCompleted++;
        return { rowCount: 0, rows: [] };
      } else if (
        sql.includes(
          'UPDATE test_schema.test_table SET SET abandoned_at = NOW(), finished_attempts = finished_attempts + 1 WHERE id = $1',
        )
      ) {
        client.markMessageAbandoned++;
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
    markMessageAbandoned: number;
  };
  return client;
}

describe('createMessageHandler', () => {
  it.each(['replication' as ListenerType, 'polling' as ListenerType])(
    'Should handle a message and mark it as processed',
    async (listenerTyp) => {
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
      const config: ReplicationConfig = {
        outboxOrInbox: 'inbox',
        dbListenerConfig: {},
        settings: {
          dbSchema: 'test_schema',
          dbTable: 'test_table',
          postgresPub: 'test_pub',
          postgresSlot: 'test_slot',
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
        listenerTyp,
      );
      const mockMessage = {
        ...message,
      };
      const cancellation = new EventEmitter();

      // Act
      await messageHandler(mockMessage, cancellation);

      // Assert
      expect(handler.handle).toHaveBeenCalledWith(mockMessage, client);
      expect(client.startedAttemptsIncrement).toBe(
        listenerTyp === 'replication' ? 1 : 0,
      );
      expect(client.initiateMessageProcessing).toBe(1);
      expect(client.markMessageCompleted).toBe(1);
      expect(
        strategies.messageProcessingTransactionLevelStrategy,
      ).toHaveBeenCalledWith(mockMessage);
      expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
      expect(strategies.messageRetryStrategy).not.toHaveBeenCalled();
    },
  );

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
    const config: ReplicationConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        postgresPub: 'test_pub',
        postgresSlot: 'test_slot',
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
      'replication',
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
    const config: ReplicationConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        postgresPub: 'test_pub',
        postgresSlot: 'test_slot',
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
      'replication',
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
    const config: ReplicationConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        postgresPub: 'test_pub',
        postgresSlot: 'test_slot',
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
      'replication',
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
    const config: ReplicationConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        postgresPub: 'test_pub',
        postgresSlot: 'test_slot',
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
      'replication',
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
    const config: ReplicationConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        postgresPub: 'test_pub',
        postgresSlot: 'test_slot',
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
      'replication',
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
    const config: ReplicationConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        postgresPub: 'test_pub',
        postgresSlot: 'test_slot',
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
      'replication',
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

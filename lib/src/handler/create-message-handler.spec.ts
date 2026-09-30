import { EventEmitter } from 'events';
import { Client } from 'pg';
import { DatabaseClient } from '../common/database';
import { getDisabledLogger } from '../common/logger';
import { StoredTransactionalMessage } from '../message/transactional-message';
import {
  FullReplicationListenerConfig,
  ReplicationListenerConfig,
} from '../replication/config';
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
          'UPDATE test_schema.test_table SET processed_at = clock_timestamp(), finished_attempts = finished_attempts + 1 WHERE id = $1',
        )
      ) {
        client.markMessageCompleted++;
        return { rowCount: 0, rows: [] };
      } else if (
        sql.includes(
          'UPDATE test_schema.test_table SET SET abandoned_at = clock_timestamp(), finished_attempts = finished_attempts + 1 WHERE id = $1',
        )
      ) {
        client.markMessageAbandoned++;
        return { rowCount: 0, rows: [] };
      } else {
        return { rowCount: 0, rows: [] }; // BEGIN, COMMIT, ...
      }
    },
    release() {},
  } as unknown as DatabaseClient & {
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
        messageNotFoundRetryStrategy: jest
          .fn()
          .mockReturnValue({ retry: false, delayInMs: 1 }),
      };
      const config: ReplicationListenerConfig = {
        outboxOrInbox: 'inbox',
        dbListenerConfig: {},
        settings: {
          dbSchema: 'test_schema',
          dbTable: 'test_table',
          dbPublication: 'test_pub',
          dbReplicationSlot: 'test_slot',
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
      messageNotFoundRetryStrategy: jest
        .fn()
        .mockReturnValue({ retry: false, delayInMs: 1 }),
    };
    const config: ReplicationListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        dbPublication: 'test_pub',
        dbReplicationSlot: 'test_slot',
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
      messageNotFoundRetryStrategy: jest
        .fn()
        .mockReturnValue({ retry: false, delayInMs: 1 }),
    };
    const config: ReplicationListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        dbPublication: 'test_pub',
        dbReplicationSlot: 'test_slot',
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
      messageNotFoundRetryStrategy: jest
        .fn()
        .mockReturnValue({ retry: false, delayInMs: 1 }),
    };
    const config: ReplicationListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        dbPublication: 'test_pub',
        dbReplicationSlot: 'test_slot',
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
      messageNotFoundRetryStrategy: jest
        .fn()
        .mockReturnValue({ retry: false, delayInMs: 1 }),
    };
    const config: ReplicationListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        dbPublication: 'test_pub',
        dbReplicationSlot: 'test_slot',
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
      messageNotFoundRetryStrategy: jest
        .fn()
        .mockReturnValue({ retry: false, delayInMs: 1 }),
    };
    const config: ReplicationListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        dbPublication: 'test_pub',
        dbReplicationSlot: 'test_slot',
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

  it('Should not double check that a message is not processed if max attempts are exceeded', async () => {
    // Arrange
    const client = getClient({ finished_attempts: 6 });
    const config: FullReplicationListenerConfig = {
      outboxOrInbox: 'inbox',
      dbHandlerConfig: {},
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        dbPublication: 'test_pub',
        dbReplicationSlot: 'test_slot',
        enablePoisonousMessageProtection: true,
        enableMaxAttemptsProtection: true,
        maxAttempts: 5,
        restartDelayInMs: 200,
        restartDelaySlotInUseInMs: 4000,
        messageProcessingTimeoutInMs: 500,
        maxPoisonousAttempts: 3,
        messageCleanupIntervalInMs: 0,
        messageCleanupProcessedInSec: 20000,
        messageCleanupAbandonedInSec: 20000,
        messageCleanupAllInSec: 20000,
        maxMessageNotFoundAttempts: 0,
        maxMessageNotFoundDelayInMs: 10,
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
      messageNotFoundRetryStrategy: jest
        .fn()
        .mockReturnValue({ retry: false, delayInMs: 1 }),
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
    expect(handler.handle).toHaveBeenCalled();
    expect(client.startedAttemptsIncrement).toBe(1);
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.markMessageCompleted).toBe(1);
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalledWith(mockMessage);
  });
});

describe('createMessageHandler message processing timeout', () => {
  interface Deferred {
    promise: Promise<void>;
    resolve: () => void;
  }
  const deferred = (): Deferred => {
    let resolve: () => void = () => {};
    const promise = new Promise<void>((res) => {
      resolve = res;
    });
    return { promise, resolve };
  };
  /** Let all currently queued promise callbacks run */
  const flush = () => new Promise((resolve) => setImmediate(resolve));

  /**
   * A client that behaves like a pg-pool client: `release` throws when it is
   * called twice for the same checkout and releasing with an error destroys the
   * connection so further queries fail.
   */
  const getPoolLikeClient = (delays: {
    commit?: Deferred;
    rollback?: Deferred;
  }) => {
    let checkedOut = true;
    let destroyed = false;
    const client = Object.assign(Object.create(Client.prototype), {
      queries: [] as string[],
      releaseCalls: [] as unknown[],
      query: async (sql: string) => {
        client.queries.push(sql);
        if (destroyed) {
          throw new Error('Client was closed and is not queryable');
        }
        if (sql.includes('FOR NO KEY UPDATE NOWAIT')) {
          return {
            rowCount: 1,
            rows: [{ started_attempts: 1, finished_attempts: 0 }],
          };
        }
        if (sql === 'COMMIT' && delays.commit) {
          await delays.commit.promise;
        }
        if (sql === 'ROLLBACK' && delays.rollback) {
          await delays.rollback.promise;
        }
        return { rowCount: 0, rows: [] };
      },
      release: (err?: Error) => {
        client.releaseCalls.push(err);
        if (!checkedOut) {
          throw new Error(
            'Release called on client which has already been released to the pool.',
          );
        }
        checkedOut = false;
        destroyed = !!err;
      },
    });
    return client;
  };

  const createHandler = (
    client: DatabaseClient,
    handle: TransactionalMessageHandler['handle'],
  ) => {
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
      messageNotFoundRetryStrategy: jest
        .fn()
        .mockReturnValue({ retry: false, delayInMs: 1 }),
    };
    const config: ReplicationListenerConfig = {
      outboxOrInbox: 'inbox',
      dbListenerConfig: {},
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        dbPublication: 'test_pub',
        dbReplicationSlot: 'test_slot',
        enablePoisonousMessageProtection: false,
        enableMaxAttemptsProtection: false,
      },
    };
    return createMessageHandler(
      { handle },
      strategies,
      config,
      getDisabledLogger(),
      'polling',
    );
  };

  it('Should not release the client again when the timeout fires after the message was processed', async () => {
    // Arrange
    const client = getPoolLikeClient({});
    const messageHandler = createHandler(client, jest.fn());
    const cancellation = new EventEmitter();
    await messageHandler({ ...message }, cancellation);
    expect(client.releaseCalls).toEqual([undefined]);

    // Act
    cancellation.emit('timeout', new Error('timeout'));
    await flush();

    // Assert
    expect(client.releaseCalls).toEqual([undefined]);
    expect(client.queries).not.toContain('ROLLBACK');
  });

  it('Should not release the client again when the transaction commits while the timeout ROLLBACK is queued', async () => {
    // Arrange
    const commit = deferred();
    const rollback = deferred();
    const client = getPoolLikeClient({ commit, rollback });
    const messageHandler = createHandler(client, jest.fn());
    const cancellation = new EventEmitter();
    const processing = messageHandler({ ...message }, cancellation);
    await flush();
    expect(client.queries).toContain('COMMIT');

    // Act: the timeout fires while the COMMIT is in flight - its ROLLBACK is
    // queued behind the COMMIT on the same connection and resolves after it.
    cancellation.emit('timeout', new Error('timeout'));
    await flush();
    commit.resolve();
    await processing;
    expect(client.releaseCalls).toEqual([undefined]);
    rollback.resolve();
    await flush();

    // Assert
    expect(client.releaseCalls).toEqual([undefined]);
  });

  it('Should release the client with the timeout error once and surface the original error when the handler finishes later', async () => {
    // Arrange
    const client = getPoolLikeClient({});
    const handlerDone = deferred();
    const messageHandler = createHandler(client, () => handlerDone.promise);
    const cancellation = new EventEmitter();
    const processing = messageHandler({ ...message }, cancellation);
    await flush();

    // Act
    cancellation.emit('timeout', new Error('timeout'));
    await flush();
    expect(client.releaseCalls).toHaveLength(1);
    expect(client.releaseCalls[0]).toMatchObject({ errorCode: 'TIMEOUT' });
    handlerDone.resolve();

    // Assert: the original error is thrown - not the pg-pool double release error
    // that the (swallowed) second release attempt in `executeTransaction` raised
    const error = await processing.catch((e) => e);
    expect(error.message).toBe('Client was closed and is not queryable');
    expect(error.errorCode).toBe('DB_ERROR');
    expect(client.releaseCalls[0]).toMatchObject({ errorCode: 'TIMEOUT' });
    expect(
      client.queries.filter((q: string) => q.includes('SET processed_at')),
    ).toEqual([]);
  });
});

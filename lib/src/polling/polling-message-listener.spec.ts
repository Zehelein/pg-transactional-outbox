/* eslint-disable @typescript-eslint/no-explicit-any */
// eslint-disable-next-line prettier/prettier
// import * as wtf from 'wtfnode';
// use `wtf.dump();` to get all open handles
// eslint-disable-next-line prettier/prettier
import inspector from 'inspector';
import { Client, PoolClient } from 'pg';
import { getDisabledLogger, getInMemoryLogger } from '../common/logger';
import { IsolationLevel, sleep } from '../common/utils';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { PollingConfig } from './config';
import * as getNextInboxMessagesImportSpy from './next-messages';
import { initializePollingMessageListener } from './polling-message-listener';
import { PollingMessageStrategies } from './polling-strategies';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000);
}

// Mock the DB client to send data and to check that it was (not) called for acknowledgement
let client: PoolClient & {
  initiateMessageProcessing: number;
  startedAttemptsIncrement: number;
  increaseMessageFinishedAttempts: number;
  markMessageCompleted: number;
  markMessageAbandoned: number;
  transactionStarts: number;
  commits: number;
  rollbacks: number;
};
let poolEvents: Record<string, (...args: unknown[]) => void> = {};
let clientEvents: Record<string, (...args: unknown[]) => void> = {};
jest.mock('pg', () => {
  return {
    Pool: jest.fn().mockImplementation(() => ({
      connect: jest.fn(() => new Client()),
      on: jest.fn((event, callback) => (poolEvents[event] = callback)),
      end: jest.fn(() => Promise.resolve()),
      removeAllListeners: jest.fn(),
    })),
    Client: jest.fn().mockImplementation(() => {
      return client;
    }),
  };
});

const getNextInboxMessagesSpy = jest.spyOn(
  getNextInboxMessagesImportSpy,
  'getNextInboxMessages',
);

const aggregateType = 'test_type';
const messageType = 'test_message_type';
const notProcessedMessage: StoredTransactionalMessage = {
  id: 'not_processed_id',
  aggregateType,
  messageType,
  aggregateId: 'test_aggregateId',
  concurrency: 'sequential',
  payload: { result: 'success' },
  metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
  createdAt: '2023-01-18T21:02:27.000Z',
  startedAttempts: 2,
  finishedAttempts: 2,
  lockedUntil: '1970-01-01T00:00:00.000Z',
  processedAt: null,
  abandonedAt: null,
};
const lastAttemptMessage: StoredTransactionalMessage = {
  id: 'last_attempt',
  aggregateType,
  messageType,
  aggregateId: 'test_aggregateId',
  concurrency: 'sequential',
  payload: { result: 'success' },
  metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
  createdAt: '2023-01-18T21:02:27.000Z',
  startedAttempts: 4,
  finishedAttempts: 4, // 5 is max by default
  lockedUntil: '1970-01-01T00:00:00.000Z',
  processedAt: null,
  abandonedAt: null, // attempts will be exceeded with next try
};
const poisonousExceededMessage: StoredTransactionalMessage = {
  id: 'poisonous_message_exceeded',
  aggregateType,
  messageType,
  aggregateId: 'test_aggregateId',
  concurrency: 'sequential',
  payload: { result: 'success' },
  metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
  createdAt: '2023-01-18T21:02:27.000Z',
  startedAttempts: 4, // maximum difference of 3
  finishedAttempts: 1,
  lockedUntil: '1970-01-01T00:00:00.000Z',
  processedAt: null,
  abandonedAt: null,
};

const config: PollingConfig = {
  outboxOrInbox: 'inbox',
  dbHandlerConfig: {
    host: 'test_host',
    port: 5432,
    database: 'test_db',
    user: 'test_inbox_handler',
    password: 'test_inbox_handler_password',
  },
  dbListenerConfig: {
    host: 'test_host',
    port: 5432,
    database: 'test_db',
    user: 'test_inbox_listener',
    password: 'test_inbox_listener_password',
  },
  settings: {
    dbSchema: 'test_schema',
    dbTable: 'test_table',
    nextMessagesBatchSize: 2,
    nextMessagesFunctionName: 'next_inbox_messages',
    nextMessagesPollingInterval: 100,
  },
};

const getQueryFunction =
  (r: {
    initiateMessageProcessingResponse?: StoredTransactionalMessage;
    custom?: (sql: string, params: [any]) => any | undefined;
  }) =>
  (sql: string, params: [any]) => {
    const customHandled = r?.custom?.(sql, params);
    if (customHandled !== undefined) {
      return customHandled;
    }
    if (
      sql.includes(
        'SELECT started_attempts, finished_attempts, processed_at, abandoned_at, locked_until FROM test_schema.test_table WHERE id = $1 FOR NO KEY UPDATE NOWAIT;',
      )
    ) {
      client.initiateMessageProcessing++;
      const mr = r.initiateMessageProcessingResponse;
      return {
        rowCount: r.initiateMessageProcessingResponse ? 1 : 0,
        rows: mr
          ? [
              {
                started_attempts: mr.startedAttempts + 1,
                finished_attempts: mr.finishedAttempts,
                processed_at: mr.processedAt ? new Date(mr.processedAt) : null,
                abandoned_at: mr.abandonedAt ? new Date(mr.abandonedAt) : null,
                locked_until: mr.lockedUntil ? new Date(mr.lockedUntil) : null,
              },
            ]
          : [],
      };
    }
    if (
      sql ===
      'UPDATE test_schema.test_table SET finished_attempts = finished_attempts + 1 WHERE id = $1;'
    ) {
      client.increaseMessageFinishedAttempts++;
      return {
        rowCount: 1,
        rows: [],
      };
    }
    if (
      sql ===
      'UPDATE test_schema.test_table SET processed_at = NOW(), finished_attempts = finished_attempts + 1 WHERE id = $1'
    ) {
      client.markMessageCompleted++;
      return {
        rowCount: 1,
        rows: [],
      };
    }
    if (
      sql ===
      'UPDATE test_schema.test_table SET abandoned_at = NOW(), finished_attempts = finished_attempts + 1 WHERE id = $1;'
    ) {
      client.markMessageAbandoned++;
      return {
        rowCount: 1,
        rows: [],
      };
    }
    if (
      sql === 'BEGIN' ||
      sql.startsWith('START TRANSACTION ISOLATION LEVEL')
    ) {
      client.transactionStarts++;
      return {
        rowCount: 0,
        rows: [],
      };
    }
    if (sql === 'COMMIT') {
      client.commits++;
      return {
        rowCount: 0,
        rows: [],
      };
    }
    if (sql === 'ROLLBACK') {
      client.rollbacks++;
      return {
        rowCount: 0,
        rows: [],
      };
    }
  };

describe('Polling message listener unit tests - initializePollingMessageListener', () => {
  let cleanup: undefined | (() => Promise<void>);
  beforeEach(() => {
    client = {
      initiateMessageProcessing: 0,
      startedAttemptsIncrement: 0,
      increaseMessageFinishedAttempts: 0,
      markMessageCompleted: 0,
      markMessageAbandoned: 0,
      transactionStarts: 0,
      commits: 0,
      rollbacks: 0,
      query: jest.fn(),
      connect: jest.fn(),
      removeAllListeners: jest.fn(),
      on: jest.fn((event, callback) => (clientEvents[event] = callback)),
      end: jest.fn(),
      release: jest.fn(),
    } as unknown as typeof client;
    poolEvents = {};
    clientEvents = {};
    getNextInboxMessagesSpy.mockReset();
  });

  afterEach(async () => {
    if (cleanup) {
      await cleanup();
      cleanup = undefined;
    }
  });

  it('should call the correct messageHandler and mark the message as processed when no errors are thrown', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve(sleep(50)));
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message = { ...notProcessedMessage };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: message,
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([
        { ...message, startedAttempts: message.startedAttempts + 1 },
      ])
      .mockResolvedValue([]);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType: message.aggregateType,
          messageType: message.messageType,
          handle: messageHandler,
        },
        {
          aggregateType: 'unused-aggregate-type',
          messageType: 'unused-message-type',
          handle: unusedMessageHandler,
        },
      ],
      getDisabledLogger(),
    );
    cleanup = shutdown;

    // Act
    await sleep(170);

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
    };
    expect(messageHandler).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
    );
    expect(messageHandler).toHaveBeenCalledTimes(1); // only one message in the queue
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(0);
    expect(client.markMessageCompleted).toBe(1);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(1);
    expect(client.commits).toBe(1);
    expect(client.rollbacks).toBe(0);
    // 100ms max wait time between tests configured. The message takes 50ms
    // --> first call immediately, second after 50ms, third after 150ms
    expect(getNextInboxMessagesSpy.mock.calls).toHaveLength(3);
  });

  it('should call the correct messageHandler and increase the finished attempts when an error is thrown', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.reject(new Error('Test')));
    const messageErrorHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message = { ...notProcessedMessage };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: message,
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([
        { ...message, startedAttempts: message.startedAttempts + 1 },
      ])
      .mockResolvedValue([]);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType: message.aggregateType,
          messageType: message.messageType,
          handle: messageHandler,
          handleError: messageErrorHandler,
        },
        {
          aggregateType: 'unused-aggregate-type',
          messageType: 'unused-message-type',
          handle: unusedMessageHandler,
        },
      ],
      getDisabledLogger(),
    );
    cleanup = shutdown;

    // Act
    await sleep(50);

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
      finishedAttempts: message.finishedAttempts + 1, // incremented in the error handler
    };
    expect(messageHandler).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
    );
    expect(messageHandler).toHaveBeenCalledTimes(1); // only one message in the queue
    expect(messageErrorHandler).toHaveBeenCalledWith(
      expect.any(Error),
      expectedMessage,
      expect.any(Object),
      true,
    );
    expect(messageErrorHandler).toHaveBeenCalledTimes(1);
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(1);
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(2); // handler + error handler
    expect(client.commits).toBe(1); // error handler
    expect(client.rollbacks).toBe(1); // handler
  });

  it('should return an error if more than one messageHandler is registered for one aggregate/message type combination', () => {
    // Arrange
    const aggregateType = 'aggregate_type';
    const messageType = 'message_type';

    // Act + Assert
    expect(() =>
      initializePollingMessageListener(
        config,
        [
          {
            aggregateType,
            messageType,
            handle: jest.fn(() => Promise.resolve()),
          },
          {
            aggregateType,
            messageType,
            handle: jest.fn(() => Promise.resolve()),
          },
        ],
        getDisabledLogger(),
      ),
    ).toThrow(
      `Only one message handler can handle one aggregate and message type. Multiple message handlers try to handle the aggregate type "${aggregateType}" with the message type "${messageType}"`,
    );

    expect(client.initiateMessageProcessing).toBe(0);
    expect(client.increaseMessageFinishedAttempts).toBe(0);
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(0);
    expect(client.commits).toBe(0);
    expect(client.rollbacks).toBe(0);
  });

  it('should raise an error if no message handlers are defined', () => {
    // Act
    expect(() =>
      initializePollingMessageListener(config, [], getDisabledLogger()),
    ).toThrow('At least one message handler must be provided');
  });

  it('should not call a messageHandler when the DB message was somehow process by another process after the started attempts increment.', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.reject(new Error('Test')));
    const nextMessage = {
      ...notProcessedMessage,
      startedAttempts: notProcessedMessage.startedAttempts + 1,
    };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: {
        ...nextMessage,
        processedAt: '2023-01-18T21:02:27.000Z', // processed between next message and init processing
      },
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([nextMessage])
      .mockResolvedValue([]);
    const timeoutMock = jest.fn(() => 100);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType,
          messageType,
          handle: messageHandler,
        },
      ],
      getDisabledLogger(),
      { messageProcessingTimeoutStrategy: timeoutMock },
    );
    cleanup = shutdown;

    // Act
    await sleep(150);

    // Assert
    expect(timeoutMock).toHaveBeenCalledWith(nextMessage); // not processed at this time
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(0);
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(1);
    expect(client.commits).toBe(1);
    expect(client.rollbacks).toBe(0);
  });

  it('should not call a messageHandler and not try to mark the message as completed when the DB message was not found during processing.', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.reject(new Error('Test')));
    const response = getQueryFunction({
      initiateMessageProcessingResponse: undefined,
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([notProcessedMessage])
      .mockResolvedValue([]);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType,
          messageType,
          handle: messageHandler,
        },
      ],
      getDisabledLogger(),
    );
    cleanup = shutdown;

    // Act
    await sleep(150);

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(0);
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(1);
    expect(client.commits).toBe(1);
    expect(client.rollbacks).toBe(0);
  });

  it('on a message handler timeout the client should do a ROLLBACK and not commit', async () => {
    // Arrange
    const messageHandler = jest.fn(async () => await sleep(250));
    const messageErrorHandler = jest.fn();
    const response = getQueryFunction({
      initiateMessageProcessingResponse: notProcessedMessage,
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([notProcessedMessage])
      .mockResolvedValue([]);
    const timeoutMock = jest.fn(() => 100);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType,
          messageType,
          handle: messageHandler,
          handleError: messageErrorHandler,
        },
      ],
      getDisabledLogger(),
      { messageProcessingTimeoutStrategy: timeoutMock },
    );
    cleanup = shutdown;

    // Act
    await sleep(500);

    // Assert
    expect(timeoutMock).toHaveBeenCalledWith(notProcessedMessage);
    expect(messageHandler).toHaveBeenCalledWith(
      notProcessedMessage,
      expect.any(Object),
    );
    expect(messageHandler).toHaveBeenCalledTimes(1);
    expect(messageErrorHandler).toHaveBeenCalledWith(
      expect.any(Error),
      notProcessedMessage,
      expect.any(Object),
      true,
    );
    expect(messageErrorHandler).toHaveBeenCalledTimes(1);
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(1);
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(2); // handler and error handler
    expect(client.commits).toBe(2); // handler (but rollback was first) and error handler
    expect(client.rollbacks).toBe(1); // handler
  });

  it('should not call the messageHandler on a poisonous message that already has exceeded the maximum poisonous retries', async () => {
    // Arrange
    const messageHandler = jest.fn();
    const nextMessage = {
      ...poisonousExceededMessage,
      startedAttempts: poisonousExceededMessage.startedAttempts + 1,
    };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: {
        ...nextMessage,
      },
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([nextMessage])
      .mockResolvedValue([]);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType,
          messageType,
          handle: messageHandler,
        },
      ],
      getDisabledLogger(),
    );
    cleanup = shutdown;

    // Act
    await sleep(150);

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.initiateMessageProcessing).toBe(0);
    expect(client.increaseMessageFinishedAttempts).toBe(0);
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(1);
    expect(client.transactionStarts).toBe(1);
    expect(client.commits).toBe(1);
    expect(client.rollbacks).toBe(0);
  });

  it.each(['config', 'strategy'])(
    'should call the messageHandler mark the message as processed even if the message is a poisonous message when poisonous handling is disabled via %p',
    async (type: string) => {
      // Arrange
      const messageHandler = jest.fn(() => Promise.resolve());
      const messageErrorHandler = jest.fn();
      const message = { ...poisonousExceededMessage };
      const response = getQueryFunction({
        initiateMessageProcessingResponse: message,
      });
      client.query = jest.fn(response) as any;
      getNextInboxMessagesSpy
        .mockResolvedValueOnce([
          { ...message, startedAttempts: message.startedAttempts + 1 },
        ])
        .mockResolvedValue([]);
      const disabledPoisonousConfig =
        type === 'config'
          ? {
              ...config,
              settings: {
                ...config.settings,
                enablePoisonousMessageProtection: false,
              },
            }
          : config;
      const disabledPoisonousStrategy =
        type === 'strategy'
          ? {
              poisonousMessageRetryStrategy: () => true,
            }
          : {};
      const [shutdown] = initializePollingMessageListener(
        disabledPoisonousConfig,
        [
          {
            aggregateType: message.aggregateType,
            messageType: message.messageType,
            handle: messageHandler,
          },
        ],
        getDisabledLogger(),
        disabledPoisonousStrategy,
      );
      cleanup = shutdown;

      // Act
      await sleep(250);

      // Assert
      const expectedMessage = {
        ...message,
        startedAttempts: message.startedAttempts + 1,
      };
      expect(messageHandler).toHaveBeenCalledWith(
        expectedMessage,
        expect.any(Object),
      );
      expect(messageHandler).toHaveBeenCalledTimes(1);
      expect(messageErrorHandler).not.toHaveBeenCalled();
      expect(client.initiateMessageProcessing).toBe(1);
      expect(client.increaseMessageFinishedAttempts).toBe(0);
      expect(client.markMessageCompleted).toBe(1);
      expect(client.markMessageAbandoned).toBe(0);
      expect(client.transactionStarts).toBe(1);
      expect(client.commits).toBe(1);
      expect(client.rollbacks).toBe(0);
    },
  );

  it('should mark a message as abandoned when there is an error and the attempts are exceeded', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.reject(new Error('Last one')));
    const messageErrorHandler = jest.fn();
    const nextMessage = {
      ...lastAttemptMessage,
      startedAttempts: lastAttemptMessage.startedAttempts + 1,
    };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: {
        ...nextMessage,
      },
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([nextMessage])
      .mockResolvedValue([]);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType,
          messageType,
          handle: messageHandler,
          handleError: messageErrorHandler,
        },
      ],
      getDisabledLogger(),
    );
    cleanup = shutdown;

    // Act
    await sleep(150);

    // Assert
    expect(messageHandler).toHaveBeenCalledWith(
      nextMessage,
      expect.any(Object),
    );
    expect(messageHandler).toHaveBeenCalledTimes(1);
    expect(messageErrorHandler).toHaveBeenCalledWith(
      expect.any(Error),
      nextMessage,
      expect.any(Object),
      false, // no more retries
    );
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(0);
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(1);
    expect(client.transactionStarts).toBe(2); // handler + error handler
    expect(client.commits).toBe(1); // handler
    expect(client.rollbacks).toBe(1); // error handler
  });

  it('should not process a message when the attempts are exceeded', async () => {
    // Arrange
    const messageHandler = jest.fn();
    const nextMessage = {
      ...lastAttemptMessage,
      startedAttempts: lastAttemptMessage.startedAttempts + 1,
      finishedAttempts: lastAttemptMessage.finishedAttempts + 1, // exceeded last attempt now
    };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: {
        ...nextMessage,
      },
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([nextMessage])
      .mockResolvedValue([]);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType,
          messageType,
          handle: messageHandler,
        },
      ],
      getDisabledLogger(),
    );
    cleanup = shutdown;

    // Act
    await sleep(150);

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(0);
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(1);
    expect(client.transactionStarts).toBe(1);
    expect(client.commits).toBe(1);
    expect(client.rollbacks).toBe(0);
  });

  it('a messageHandler throws an error and the error handler throws an error as well the message should still increase attempts', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.reject(new Error('Handler')));
    const messageErrorHandler = jest.fn(() =>
      Promise.reject(new Error('Error Handler')),
    );
    const nextMessage = {
      ...notProcessedMessage,
      startedAttempts: notProcessedMessage.startedAttempts + 1,
    };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: {
        ...nextMessage,
      },
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([nextMessage])
      .mockResolvedValue([]);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType,
          messageType,
          handle: messageHandler,
          handleError: messageErrorHandler,
        },
      ],
      getDisabledLogger(),
    );
    cleanup = shutdown;

    // Act
    await sleep(150);

    // Assert
    expect(messageHandler).toHaveBeenCalledWith(
      nextMessage,
      expect.any(Object),
    );
    expect(messageHandler).toHaveBeenCalledTimes(1);
    expect(messageErrorHandler).toHaveBeenCalledWith(
      expect.any(Error),
      nextMessage,
      expect.any(Object),
      true,
    );
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(1); // only in best effort - the error handler throws
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(3); // handler + error handler + best effort error handling
    expect(client.commits).toBe(1); // best effort increase
    expect(client.rollbacks).toBe(2); // handler and error handler
  });

  it('a message handler throws an error and the error handler throws an error and the best effort finished attempt increase throws as well there should be a warning log', async () => {
    // Arrange
    const [logger, logs] = getInMemoryLogger('unit test');
    const messageHandler = jest.fn(() => Promise.reject(new Error('Handler')));
    const messageErrorHandler = jest.fn(() =>
      Promise.reject(new Error('Error Handler')),
    );
    const nextMessage = {
      ...notProcessedMessage,
      startedAttempts: notProcessedMessage.startedAttempts + 1,
    };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: {
        ...nextMessage,
      },
      custom: (sql: string) => {
        if (
          sql ===
          'UPDATE test_schema.test_table SET finished_attempts = finished_attempts + 1 WHERE id = $1;'
        ) {
          // is only called in the best-effort case as the error message handler already throws before
          client.increaseMessageFinishedAttempts++;
          throw Error('Best effort error');
        }
      },
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([nextMessage])
      .mockResolvedValue([]);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType,
          messageType,
          handle: messageHandler,
          handleError: messageErrorHandler,
        },
      ],
      logger,
    );
    cleanup = shutdown;

    // Act
    await sleep(150);

    // Assert
    expect(messageHandler).toHaveBeenCalledWith(
      nextMessage,
      expect.any(Object),
    );
    expect(messageHandler).toHaveBeenCalledTimes(1);
    expect(messageErrorHandler).toHaveBeenCalledWith(
      expect.any(Error),
      nextMessage,
      expect.any(Object),
      true,
    );
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(1); // only in best effort - the error handler throws
    expect(client.markMessageCompleted).toBe(0);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(3); // handler + error handler + best effort error handling
    expect(client.commits).toBe(0);
    expect(client.rollbacks).toBe(3); // error handler and best effort
    const log = logs.filter(
      (log) =>
        log.args[1] ===
        `The 'best-effort' logic to increase the ${config.outboxOrInbox} message finished attempts failed as well.`,
    );
    expect(log).toHaveLength(1);
  });

  it('should log a debug message when no messageHandler was found for a message', async () => {
    // Arrange
    const [logger, logs] = getInMemoryLogger('unit test');
    const messageHandler = jest.fn();
    const nextMessage = {
      ...notProcessedMessage,
      startedAttempts: notProcessedMessage.startedAttempts + 1,
    };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: {
        ...nextMessage,
        processedAt: '2023-01-18T21:02:27.000Z', // processed between next message and init processing
      },
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([nextMessage])
      .mockResolvedValue([]);
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType: 'not-found',
          messageType: 'not-found',
          handle: messageHandler,
        },
      ],
      logger,
    );
    cleanup = shutdown;

    // Act
    await sleep(150);

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.initiateMessageProcessing).toBe(0);
    expect(client.increaseMessageFinishedAttempts).toBe(0);
    expect(client.markMessageCompleted).toBe(1);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(1);
    expect(client.commits).toBe(1);
    expect(client.rollbacks).toBe(0);
    const log = logs.filter(
      (log) =>
        log.args[0] ===
        'No inbox message handler found for aggregate type "test_type" and message tye "test_message_type"',
    );
    expect(log).toHaveLength(1);
  });

  it('should use all the strategies', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const message = { ...notProcessedMessage };
    const response = getQueryFunction({
      initiateMessageProcessingResponse: message,
    });
    client.query = jest.fn(response) as any;
    getNextInboxMessagesSpy
      .mockResolvedValueOnce([
        { ...message, startedAttempts: message.startedAttempts + 1 },
      ])
      .mockResolvedValue([]);
    const strategies: PollingMessageStrategies = {
      messageProcessingDbClientStrategy: {
        getClient: jest.fn().mockReturnValue(client),
        shutdown: jest.fn(),
      },
      messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(2_000),
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(IsolationLevel.Serializable),
      messageRetryStrategy: jest.fn().mockReturnValue(true),
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(true),
      batchSizeStrategy: jest.fn().mockReturnValue(2),
    };
    const [shutdown] = initializePollingMessageListener(
      config,
      [
        {
          aggregateType: message.aggregateType,
          messageType: message.messageType,
          handle: messageHandler,
        },
      ],
      getDisabledLogger(),
      strategies,
    );
    cleanup = shutdown;

    // Act
    await sleep(250);

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
    };
    expect(messageHandler).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
    );
    expect(messageHandler).toHaveBeenCalledTimes(1); // only one message in the queue
    expect(client.initiateMessageProcessing).toBe(1);
    expect(client.increaseMessageFinishedAttempts).toBe(0);
    expect(client.markMessageCompleted).toBe(1);
    expect(client.markMessageAbandoned).toBe(0);
    expect(client.transactionStarts).toBe(1);
    expect(client.commits).toBe(1);
    expect(client.rollbacks).toBe(0);
    expect(
      strategies.messageProcessingDbClientStrategy.getClient,
    ).toHaveBeenCalled();
    expect(
      strategies.messageProcessingDbClientStrategy.shutdown,
    ).not.toHaveBeenCalled();
    expect(strategies.messageProcessingTimeoutStrategy).toHaveBeenCalled();
    expect(
      strategies.messageProcessingTransactionLevelStrategy,
    ).toHaveBeenCalled();
    expect(strategies.messageRetryStrategy).toHaveBeenCalled();
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.batchSizeStrategy).toHaveBeenCalled();
  });

  it('should correctly handle database event callbacks', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve(sleep(50)));
    getNextInboxMessagesSpy.mockResolvedValue([]);
    client.query = jest.fn(getQueryFunction({})) as any;
    const [shutdown] = initializePollingMessageListener(
      config,
      {
        handle: messageHandler,
      },
      getDisabledLogger(),
    );
    cleanup = shutdown;

    // Assert
    expect(() => poolEvents['connect'](client)).not.toThrow();
    expect(() => poolEvents['error'](new Error('test'))).not.toThrow();
    expect(() => clientEvents['notice']('some message')).not.toThrow();
  });

  it('should restart the polling functionality in case of an error', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve(sleep(50)));
    // do not resolve the mock --> getNextInboxMessagesSpy.mockResolvedValue([]);
    client.query = jest.fn(getQueryFunction({})) as any;
    const logger = getDisabledLogger();
    logger.error = jest.fn();
    let once = false;
    logger.trace = ((_object: unknown, message: string) => {
      if (message === 'Error when working on a batch of inbox messages.') {
        if (once) {
          throw new Error('restart...');
        } else {
          once = true;
        }
      }
    }) as any;

    // Act
    const [shutdown] = initializePollingMessageListener(
      config,
      {
        handle: messageHandler,
      },
      logger,
    );
    cleanup = shutdown;

    // Assert
    await sleep(1_200);
    expect(logger.error).toHaveBeenCalledWith(
      expect.any(Error),
      'Error polling for inbox messages.',
    );
  });
});

/* eslint-disable @typescript-eslint/no-explicit-any */
// eslint-disable-next-line prettier/prettier
// import * as wtf from 'wtfnode';
// use `wtf.dump();` to get all open handles
// eslint-disable-next-line prettier/prettier
import EventEmitter from 'events';
import inspector from 'inspector';
import { Client, Connection, PoolClient } from 'pg';
import { Pgoutput } from 'pg-logical-replication';
import { getDisabledLogger, getInMemoryLogger } from '../common/logger';
import { InboxMessage } from '../common/message';
import { IsolationLevel, sleep } from '../common/utils';
import { defaultMessageProcessingDbClientStrategy } from '../strategies/message-processing-db-client-strategy';
import {
  InboxConfig,
  InboxStrategies,
  initializeInboxListener,
} from './inbox-listener';
import * as inboxStorageSpy from './inbox-message-storage';

type MessageIdType =
  | 'not_processed_id'
  | 'processed_id'
  | 'attempts_exceeded'
  | 'poisonous_message_exceeded';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000);
}

const continueEventLoop = () => sleep(1);

type ReplicationClient = Client & {
  connection: Connection & { sendCopyFromChunk: (buffer: Buffer) => void };
};

// Mock the replication plugin to parse the input always as outboxDbMessage
jest.mock('pg-logical-replication', () => {
  return {
    PgoutputPlugin: jest.fn().mockImplementation(() => {
      return {
        parse: jest.fn((buffer: Buffer) => {
          return {
            tag: 'insert',
            relation,
            new: inboxMessageByFlag(buffer),
          };
        }),
        start: async () => {
          await new Promise(() => true);
        },
      };
    }),
  };
});

// Mock the DB client to send data and to check that it was (not) called for acknowledgement
let client: ReplicationClient;
jest.mock('pg', () => {
  return {
    Pool: jest.fn().mockImplementation(() => ({
      connect: jest.fn(() => new Client()),
      on: jest.fn(),
      end: jest.fn(() => Promise.resolve()),
      removeAllListeners: jest.fn(),
    })),
    Client: jest.fn().mockImplementation(() => {
      return client;
    }),
  };
});

// Send a valid chunk that represents a log message
const sendReplicationChunk = (id: MessageIdType) => {
  const data = [
    119, 0, 0, 0, 0, 93, 162, 168, 0, 0, 0, 0, 0, 9, 162, 168, 0, 0, 2, 168, 74,
    108, 17, 127, 72, 66, 0, 0, 0, 0, 9, 162, 254, 96, 0, 2, 168, 74, 108, 17,
    119, 203, 0, 1, 233, 183,
  ];
  // The first 25 bits are cut off so it starts at "66" what goes into plugin parse
  // Set this position to a specific number to map it to a message
  switch (id) {
    case 'processed_id':
      data[25] = 0;
      break;
    case 'not_processed_id':
      data[25] = 1;
      break;
    case 'attempts_exceeded':
      data[25] = 2;
      break;
    case 'poisonous_message_exceeded':
      data[25] = 3;
      break;
  }
  const chunk = Buffer.from(data);
  (client as any).connection.emit('copyData', {
    chunk,
    length: chunk.length,
    name: 'copyData',
  });
};

const markInboxMessageCompletedSpy = jest.spyOn(
  inboxStorageSpy,
  'markInboxMessageCompleted',
);
const increaseInboxMessageFinishedAttemptsSpy = jest.spyOn(
  inboxStorageSpy,
  'increaseInboxMessageFinishedAttempts',
);

jest.mock('../common/utils', () => {
  return {
    ...jest.requireActual('../common/utils'),
    executeTransaction: jest.fn(
      async (
        client: PoolClient,
        callback: (client: PoolClient) => Promise<unknown>,
      ) => {
        const response = await callback(client);
        client.release();
        return response;
      },
    ),
  };
});

const aggregate_type = 'test_type';
const message_type = 'test_message_type';
const inboxDbMessages = [
  {
    id: 'not_processed_id' as MessageIdType,
    aggregate_type,
    message_type,
    aggregate_id: 'test_aggregate_id',
    payload: { result: 'success' },
    metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    started_attempts: 2,
    finished_attempts: 2,
    processed_at: null,
  },
  {
    id: 'processed_id' as MessageIdType,
    aggregate_type,
    message_type,
    aggregate_id: 'test_aggregate_id',
    payload: { result: 'success' },
    metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    started_attempts: 0,
    finished_attempts: 0,
    processed_at: new Date('2023-01-18T21:02:27.000Z'),
  },
  {
    id: 'attempts_exceeded' as MessageIdType,
    aggregate_type,
    message_type,
    aggregate_id: 'test_aggregate_id',
    payload: { result: 'success' },
    metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    started_attempts: 4,
    finished_attempts: 4, // 5 is max by default
    processed_at: null,
  },
  {
    id: 'poisonous_message_exceeded' as MessageIdType,
    aggregate_type,
    message_type,
    aggregate_id: 'test_aggregate_id',
    payload: { result: 'success' },
    metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    started_attempts: 4, // maximum difference of 3
    finished_attempts: 1,
    processed_at: null,
  },
];

/** The message directly from the DB with started_attempts/finished_attempts/processed_at */
const inboxDbMessageById = (id: MessageIdType) =>
  inboxDbMessages.find((m) => m.id === id);

/** The message from the WAL having no started_attempts/finished_attempts/processed_at */
const inboxMessageById = (id: MessageIdType) => {
  const message = inboxDbMessageById(id);
  if (!message) {
    return;
  }
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { started_attempts, finished_attempts, processed_at, ...messageRest } =
    message;
  return messageRest;
};

const inboxMessageByFlag = (buffer: Buffer) => {
  switch (buffer[0]) {
    case 0:
      return inboxMessageById('processed_id');
    case 1:
      return inboxMessageById('not_processed_id');
    case 2:
      return inboxMessageById('attempts_exceeded');
    case 3:
      return inboxMessageById('poisonous_message_exceeded');
  }
};

const config: InboxConfig = {
  pgConfig: {
    host: 'test_host',
    port: 5432,
    database: 'test_db',
    user: 'test_user',
    password: 'test_password',
  },
  pgReplicationConfig: {
    host: 'test_host',
    port: 5432,
    database: 'test_db',
    user: 'test_inbox_user',
    password: 'test_inbox_user_password',
  },
  settings: {
    dbSchema: 'test_schema',
    dbTable: 'test_table',
    postgresPub: 'test_pub',
    postgresSlot: 'test_slot',
  },
};

const relation: Pgoutput.MessageRelation = {
  tag: 'relation',
  relationOid: 1,
  schema: 'test_schema',
  name: 'test_table',
  replicaIdentity: 'default',
  columns: [
    {
      name: 'id',
      flags: 0,
      typeOid: 23,
      typeMod: -1,
      typeSchema: 'pg_catalog',
      typeName: 'int4',
      parser: (raw: any) => raw,
    },
  ],
  keyColumns: ['id'],
};

describe('Inbox service unit tests - initializeInboxService', () => {
  beforeEach(() => {
    const connection = new EventEmitter();
    (connection as any).sendCopyFromChunk = jest.fn();

    client = {
      query: jest.fn((sql: string, params: [any]) => {
        if (
          sql.includes(
            'UPDATE test_schema.test_table SET started_attempts = started_attempts + 1 WHERE id IN',
          )
        ) {
          // startedAttemptsIncrement
          const dbMessage = inboxDbMessageById(params[0] as MessageIdType);
          return {
            rowCount: dbMessage ? 1 : 0,
            rows: dbMessage
              ? [
                  {
                    started_attempts: dbMessage.started_attempts + 1,
                    finished_attempts: dbMessage.finished_attempts,
                    processed_at: dbMessage.processed_at,
                  },
                ]
              : [],
          };
        } else if (
          sql.includes(
            'SELECT started_attempts, finished_attempts, processed_at FROM test_schema.test_table WHERE id = $1 FOR UPDATE NOWAIT;',
          )
        ) {
          // initiateInboxMessageProcessing
          const dbMessage = inboxDbMessageById(params[0] as MessageIdType);
          return {
            rowCount: dbMessage ? 1 : 0,
            rows: dbMessage
              ? [
                  {
                    started_attempts: dbMessage.started_attempts + 1,
                    finished_attempts: dbMessage.finished_attempts,
                    processed_at: dbMessage.processed_at,
                  },
                ]
              : [],
          };
        } else if (sql === 'ROLLBACK') {
          return { rowCount: 0 };
        } else {
          throw new Error(`Found an SQL query that was not mocked: ${sql}`);
        }
      }),
      connect: jest.fn(),
      connection,
      removeAllListeners: jest.fn(),
      on: jest.fn(),
      end: jest.fn(),
      release: jest.fn(),
    } as unknown as ReplicationClient;

    markInboxMessageCompletedSpy.mockReset();
    increaseInboxMessageFinishedAttemptsSpy.mockReset();
  });

  it('should call the correct messageHandler and acknowledge the WAL message when no errors are thrown', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
    };
    const [cleanup] = initializeInboxListener(
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

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
    };
    expect(messageHandler).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
    );
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    expect(markInboxMessageCompletedSpy).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
      expect.any(Object),
    );
    expect(increaseInboxMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should return an error if more than one messageHandler is registered for one aggregate/message type combination', () => {
    // Arrange
    const aggregateType = 'aggregate_type';
    const messageType = 'message_type';

    // Act + Assert
    expect(() =>
      initializeInboxListener(
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
  });

  it('should not call a messageHandler but acknowledge the WAL message when the inbox DB message was already process.', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const [cleanup] = initializeInboxListener(
      config,
      [
        {
          aggregateType: aggregate_type,
          messageType: message_type,
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

    // Act
    sendReplicationChunk('processed_id');
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    // As the inbox DB message was already processed --> no ack/nack needed
    expect(markInboxMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseInboxMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should not call a messageHandler but acknowledge the WAL message when the inbox DB message was somehow process by another process after the started attempts increment.', async () => {
    // Arrange
    client.query = jest
      .fn()
      // the call in startedAttemptsIncrement
      .mockReturnValueOnce({
        rowCount: 1,
        rows: [
          {
            started_attempts: 1,
            finished_attempts: 0,
            processed_at: null,
          },
        ],
      })
      // the call in initiateInboxMessageProcessing
      .mockReturnValueOnce({
        rowCount: 1,
        rows: [
          {
            started_attempts: 1,
            finished_attempts: 0,
            processed_at: new Date().toISOString(),
          },
        ],
      });
    const messageHandler = jest.fn(() => Promise.resolve());
    const [cleanup] = initializeInboxListener(
      config,
      [
        {
          aggregateType: aggregate_type,
          messageType: message_type,
          handle: messageHandler,
        },
      ],
      getDisabledLogger(),
    );

    // Act
    sendReplicationChunk('processed_id');
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    // As the inbox DB message was already processed --> no ack/nack needed
    expect(markInboxMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseInboxMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should not call a messageHandler and not acknowledge the WAL message when the inbox DB message was not found.', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const [cleanup] = initializeInboxListener(
      config,
      [
        {
          aggregateType: aggregate_type,
          messageType: message_type,
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

    // Act
    sendReplicationChunk('not_found' as MessageIdType);
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalled();
    // As the inbox DB message was not found --> no ack/nack needed
    expect(markInboxMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseInboxMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should call a messageHandler on a not processed message but not acknowledge the WAL message when the message handler throws an error', async () => {
    // Arrange
    const handleError = jest.fn();
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const unusedErrorHandler = jest.fn();
    const message = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
    };
    const [cleanup] = initializeInboxListener(
      config,
      [
        {
          aggregateType: message.aggregateType,
          messageType: message.messageType,
          handle: () => {
            throw new Error('Unit Test');
          },
          handleError,
        },
        {
          aggregateType: 'unused-aggregate-type',
          messageType: 'unused-message-type',
          handle: unusedMessageHandler,
          handleError: unusedErrorHandler,
        },
      ],
      getDisabledLogger(),
    );

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
      finishedAttempts: message.finishedAttempts + 1,
    };
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(unusedErrorHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalledWith();
    expect(markInboxMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseInboxMessageFinishedAttemptsSpy).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
      expect.any(Object),
    );
    expect(handleError).toHaveBeenCalledWith(
      expect.any(Object),
      expectedMessage,
      expect.any(Object),
      true,
    );
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(2); // once as part of error handling - once via shutdown
  });

  it('should call the messageHandler and acknowledge the WAL message even if the started attempts are much higher than the finished when poisonous message protection is disabled', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const message = {
      id: 'poisonous_message_exceeded',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 99, // would be a poisonous message
      finishedAttempts: 0,
      processedAt: null,
    };
    const cfg = {
      pgConfig: {
        ...config.pgConfig,
      },
      pgReplicationConfig: {
        ...config.pgReplicationConfig,
      },
      settings: { ...config.settings, enablePoisonousMessageProtection: false },
    };
    client.query = jest.fn().mockReturnValueOnce({
      rowCount: 1,
      rows: [
        {
          started_attempts: 99,
          finished_attempts: 1,
          processed_at: null,
        },
      ],
    });
    const strategies: Partial<InboxStrategies> = {
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(false), // always treat it as poisonous
    };
    const [cleanup] = initializeInboxListener(
      cfg,
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

    // Act
    sendReplicationChunk('poisonous_message_exceeded');
    await continueEventLoop();

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts, // not incremented
      finishedAttempts: message.finishedAttempts + 1, // incremented
    };
    expect(messageHandler).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
    );
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    expect(markInboxMessageCompletedSpy).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
      expect.any(Object),
    );
    expect(increaseInboxMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should not call the messageHandler on a poisonous message that already has exceeded the maximum poisonous retries', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const message: InboxMessage = {
      id: 'poisonous_message_exceeded',
      aggregateType: aggregate_type,
      messageType: message_type,
      aggregateId: 'test_aggregate_id',
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 4,
      finishedAttempts: 1,
      processedAt: null,
    };
    const [cleanup] = initializeInboxListener(
      config,
      [
        {
          aggregateType: message.aggregateType,
          messageType: message.messageType,
          handle: messageHandler,
        },
      ],
      getDisabledLogger(),
    );

    // Act
    sendReplicationChunk('poisonous_message_exceeded');
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    expect(markInboxMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseInboxMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should not retry a message when there is an error and the attempts are exceeded', async () => {
    // Arrange
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message = {
      id: 'attempts_exceeded',
      aggregateType: aggregate_type,
      messageType: message_type,
      aggregateId: 'test_aggregate_id',
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 4,
      finishedAttempts: 4,
      processedAt: null,
    };
    const [cleanup] = initializeInboxListener(
      config,
      [
        {
          aggregateType: message.aggregateType,
          messageType: message.messageType,
          handle: () => {
            throw new Error('Unit Test');
          },
        },
        {
          aggregateType: 'unused-aggregate-type',
          messageType: 'unused-message-type',
          handle: unusedMessageHandler,
        },
      ],
      getDisabledLogger(),
    );

    // Act
    sendReplicationChunk('attempts_exceeded');
    await continueEventLoop();

    // Assert
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalledWith();
    expect(markInboxMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseInboxMessageFinishedAttemptsSpy).toHaveBeenCalledWith(
      {
        ...message,
        finishedAttempts: message.finishedAttempts + 1,
        startedAttempts: message.startedAttempts + 1,
      },
      expect.any(Object),
      expect.any(Object),
    );
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should not process a message when the attempts are exceeded', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const cfg = { ...config, settings: { ...config.settings, maxAttempts: 4 } };
    const [cleanup] = initializeInboxListener(
      cfg,
      [
        {
          aggregateType: aggregate_type,
          messageType: message_type,
          handle: messageHandler,
        },
      ],
      getDisabledLogger(),
    );

    // Act
    sendReplicationChunk('attempts_exceeded');
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalledWith();
    expect(markInboxMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseInboxMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('a messageHandler throws an error and the error handler throws an error as well the message should still increase attempts', async () => {
    // Arrange
    const handleError = jest.fn().mockImplementationOnce(() => {
      throw new Error('Error handling error');
    });
    const message = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
    };
    const [logger, logs] = getInMemoryLogger('unit test');
    const [cleanup] = initializeInboxListener(
      config,
      [
        {
          aggregateType: message.aggregateType,
          messageType: message.messageType,
          handle: () => {
            throw new Error('Unit Test');
          },
          handleError,
        },
      ],
      logger,
    );

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
      finishedAttempts: message.finishedAttempts + 1,
    };
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalledWith();
    expect(markInboxMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseInboxMessageFinishedAttemptsSpy).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
      expect.any(Object),
    );
    expect(handleError).toHaveBeenCalledWith(
      expect.any(Object),
      expectedMessage,
      expect.any(Object),
      true,
    );
    expect(client.connect).toHaveBeenCalledTimes(1);
    expect(client.end).toHaveBeenCalledTimes(1);
    const log = logs.filter(
      (log) =>
        log.args[1] ===
        'The error handling of the message failed. Please make sure that your error handling code does not throw an error!',
    );
    expect(log).toHaveLength(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(2); // once as part of error handling - once via shutdown
  });

  it('a messageHandler throws an error and the error handler throws an error and the best effort increment fails also then do not retry the message', async () => {
    // Arrange
    const handleError = jest.fn().mockImplementationOnce(() => {
      throw new Error('Error handling error');
    });
    const dbClientStrategy = defaultMessageProcessingDbClientStrategy(
      config,
      getDisabledLogger(),
    );
    const message = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
    };
    const [logger, logs] = getInMemoryLogger('unit test');
    const [cleanup] = initializeInboxListener(
      config,
      [
        {
          aggregateType: message.aggregateType,
          messageType: message.messageType,
          handle: () => {
            throw new Error('Unit Test');
          },
          handleError,
        },
      ],
      logger,
      {
        messageProcessingDbClientStrategy: {
          getClient: jest
            .fn()
            .mockReturnValueOnce(dbClientStrategy.getClient(message)) // startedAttemptsIncrement
            .mockReturnValueOnce(dbClientStrategy.getClient(message)) // initiateInboxMessageProcessing
            .mockReturnValueOnce(dbClientStrategy.getClient(message)) // handleError
            .mockReturnValueOnce(null), // best effort increment - which should fail
          shutdown: dbClientStrategy.shutdown,
        },
      },
    );

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
      finishedAttempts: message.finishedAttempts + 1,
    };
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalledWith();
    expect(markInboxMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseInboxMessageFinishedAttemptsSpy).toHaveBeenCalledTimes(1);
    expect(increaseInboxMessageFinishedAttemptsSpy).toHaveBeenCalledWith(
      expectedMessage,
      null,
      expect.any(Object),
    );
    expect(handleError).toHaveBeenCalledWith(
      expect.any(Object),
      expectedMessage,
      expect.any(Object),
      true,
    );
    expect(client.connect).toHaveBeenCalledTimes(1);
    const log = logs.filter(
      (log) =>
        log.args[1] ===
        'The error handling of the message failed. Please make sure that your error handling code does not throw an error!',
    );
    expect(log).toHaveLength(1);
    await cleanup();
  });

  it('should log a debug message when no messageHandler was found for a message', async () => {
    // Arrange
    const [logger, logs] = getInMemoryLogger('unit test');
    const messageHandler = jest.fn(() => Promise.resolve());
    const message = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
    };
    const [cleanup] = initializeInboxListener(
      config,
      [
        {
          aggregateType: 'different-type',
          messageType: 'different-type',
          handle: messageHandler,
        },
      ],
      logger,
    );

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
    };
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    expect(markInboxMessageCompletedSpy).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
      expect.any(Object),
    );
    expect(increaseInboxMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    const log = logs.filter(
      (log) =>
        log.args[0] ===
        'No message handler found for aggregate type "test_type" and message tye "test_message_type"',
    );
    expect(log).toHaveLength(1);
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should raise an error if no message handlers are defined', () => {
    // Act
    expect(() =>
      initializeInboxListener(config, [], getDisabledLogger()),
    ).toThrow('At least one message handler must be provided');
  });

  it('should use all the strategies', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
    };
    const strategies: InboxStrategies = {
      concurrencyStrategy: {
        acquire: jest.fn().mockReturnValue(() => {
          /** just release */
        }),
        cancel: jest.fn(),
      },
      messageProcessingDbClientStrategy: {
        getClient: jest.fn().mockReturnValue(new Client()),
        shutdown: jest.fn(),
      },
      messageProcessingTimeoutStrategy: jest.fn().mockReturnValue(2_000),
      messageProcessingTransactionLevelStrategy: jest
        .fn()
        .mockReturnValue(IsolationLevel.Serializable),
      messageRetryStrategy: jest.fn().mockReturnValue(true),
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(true),
      listenerRestartStrategy: jest.fn().mockReturnValue(123),
    };
    const [cleanup] = initializeInboxListener(
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

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
    };
    expect(messageHandler).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
    );
    expect(strategies.concurrencyStrategy.acquire).toHaveBeenCalled();
    expect(strategies.concurrencyStrategy.cancel).not.toHaveBeenCalled();
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
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    expect(markInboxMessageCompletedSpy).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
      expect.any(Object),
    );
    expect(increaseInboxMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });
});

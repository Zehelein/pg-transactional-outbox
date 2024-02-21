/* eslint-disable @typescript-eslint/no-explicit-any */
// eslint-disable-next-line prettier/prettier
// import * as wtf from 'wtfnode';
// use `wtf.dump();` to get all open handles
// eslint-disable-next-line prettier/prettier
import EventEmitter from 'events';
import inspector from 'inspector';
import { Client, Connection } from 'pg';
import { Pgoutput } from 'pg-logical-replication';
import { DatabaseClient, releaseIfPoolClient } from '../common/database';
import { getDisabledLogger, getInMemoryLogger } from '../common/logger';
import { IsolationLevel, sleep } from '../common/utils';
import * as increaseFinishedAttemptsImportSpy from '../message/increase-message-finished-attempts';
import * as markMessageAbandonedImportSpy from '../message/mark-message-abandoned';
import * as markMessageCompletedImportSpy from '../message/mark-message-completed';
import {
  StoredTransactionalMessage,
  TransactionalMessage,
} from '../message/transactional-message';
import { defaultMessageProcessingDbClientStrategy } from '../strategies/message-processing-db-client-strategy';
import {
  FullReplicationListenerConfig,
  ReplicationListenerConfig,
} from './config';
import { initializeReplicationMessageListener } from './replication-message-listener';
import { ReplicationMessageStrategies } from './replication-strategies';

type MessageIdType =
  | 'not_processed_id'
  | 'processed_id'
  | 'last_attempt'
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
            new: messageByFlag(buffer),
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
    case 'last_attempt':
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

const markMessageCompletedSpy = jest.spyOn(
  markMessageCompletedImportSpy,
  'markMessageCompleted',
);
const increaseMessageFinishedAttemptsSpy = jest.spyOn(
  increaseFinishedAttemptsImportSpy,
  'increaseMessageFinishedAttempts',
);

const markMessageAbandonedSpy = jest.spyOn(
  markMessageAbandonedImportSpy,
  'markMessageAbandoned',
);

jest.mock('../common/utils', () => {
  return {
    ...jest.requireActual('../common/utils'),
    executeTransaction: jest.fn(
      async (
        client: DatabaseClient,
        callback: (client: DatabaseClient) => Promise<unknown>,
      ) => {
        const response = await callback(client);
        releaseIfPoolClient(client);
        return response;
      },
    ),
  };
});

const aggregate_type = 'test_type';
const message_type = 'test_message_type';
const storedDbMessages = [
  {
    id: 'not_processed_id' as MessageIdType,
    aggregate_type,
    message_type,
    aggregate_id: 'test_aggregate_id',
    concurrency: 'sequential',
    payload: { result: 'success' },
    metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    started_attempts: 2,
    finished_attempts: 2,
    locked_until: new Date('1970-01-01T00:00:00.000Z'),
    processed_at: null,
    abandoned_at: null,
  },
  {
    id: 'processed_id' as MessageIdType,
    aggregate_type,
    message_type,
    aggregate_id: 'test_aggregate_id',
    concurrency: 'sequential',
    payload: { result: 'success' },
    metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    started_attempts: 0,
    finished_attempts: 0,
    locked_until: new Date('1970-01-01T00:00:00.000Z'),
    processed_at: new Date('2023-01-18T21:02:27.000Z'),
    abandoned_at: null,
  },
  {
    id: 'last_attempt' as MessageIdType,
    aggregate_type,
    message_type,
    aggregate_id: 'test_aggregate_id',
    concurrency: 'sequential',
    payload: { result: 'success' },
    metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    started_attempts: 4,
    finished_attempts: 4, // 5 is max by default
    locked_until: new Date('1970-01-01T00:00:00.000Z'),
    processed_at: null,
    abandoned_at: null, // attempts will be exceeded with next try
  },
  {
    id: 'poisonous_message_exceeded' as MessageIdType,
    aggregate_type,
    message_type,
    aggregate_id: 'test_aggregate_id',
    concurrency: 'sequential',
    payload: { result: 'success' },
    metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    started_attempts: 4, // maximum difference of 3
    finished_attempts: 1,
    locked_until: new Date('1970-01-01T00:00:00.000Z'),
    processed_at: null,
    abandoned_at: null,
  },
];

/** The message directly from the DB with started_attempts/finished_attempts/... */
const storedDbMessageById = (id: MessageIdType) =>
  storedDbMessages.find((m) => m.id === id);

/** The message from the WAL having no started_attempts/finished_attempts/... */
const storedMessageById = (id: MessageIdType) => {
  const message = storedDbMessageById(id);
  if (!message) {
    return;
  }
  const {
    /* eslint-disable @typescript-eslint/no-unused-vars */
    started_attempts,
    finished_attempts,
    processed_at,
    abandoned_at,
    /* eslint-enable @typescript-eslint/no-unused-vars */
    ...messageRest
  } = message;
  return messageRest;
};

const messageByFlag = (buffer: Buffer) => {
  switch (buffer[0]) {
    case 0:
      return storedMessageById('processed_id');
    case 1:
      return storedMessageById('not_processed_id');
    case 2:
      return storedMessageById('last_attempt');
    case 3:
      return storedMessageById('poisonous_message_exceeded');
  }
};

const config: FullReplicationListenerConfig = {
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
    dbPublication: 'test_pub',
    dbReplicationSlot: 'test_slot',
    enableMaxAttemptsProtection: true,
    enablePoisonousMessageProtection: true,
    restartDelayInMs: 100,
    restartDelaySlotInUseInMs: 200,
    messageProcessingTimeoutInMs: 300,
    maxAttempts: 5,
    maxPoisonousAttempts: 3,
    messageCleanupIntervalInMs: 400,
    messageCleanupProcessedInSec: 500,
    messageCleanupAbandonedInSec: 600,
    messageCleanupAllInSec: 700,
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

describe('Replication message listener unit tests - initializeReplicationMessageListener', () => {
  let afterCleanup: (() => Promise<void>) | undefined = undefined;
  beforeEach(() => {
    afterCleanup = undefined;
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
          const dbMessage = storedDbMessageById(params[0] as MessageIdType);
          return {
            rowCount: dbMessage ? 1 : 0,
            rows: dbMessage
              ? [
                  {
                    started_attempts: dbMessage.started_attempts + 1,
                    finished_attempts: dbMessage.finished_attempts,
                    processed_at: dbMessage.processed_at,
                    abandoned_at: dbMessage.abandoned_at,
                    locked_until: dbMessage.locked_until,
                  },
                ]
              : [],
          };
        } else if (
          sql.includes(
            'SELECT started_attempts, finished_attempts, processed_at, abandoned_at, locked_until FROM test_schema.test_table WHERE id = $1 FOR NO KEY UPDATE NOWAIT;',
          )
        ) {
          // initiateMessageProcessing
          const dbMessage = storedDbMessageById(params[0] as MessageIdType);
          return {
            rowCount: dbMessage ? 1 : 0,
            rows: dbMessage
              ? [
                  {
                    started_attempts: dbMessage.started_attempts + 1,
                    finished_attempts: dbMessage.finished_attempts,
                    processed_at: dbMessage.processed_at,
                    abandoned_at: dbMessage.abandoned_at,
                    locked_until: dbMessage.locked_until,
                  },
                ]
              : [],
          };
        } else if (
          sql.includes(
            'UPDATE test_schema.test_table SET abandoned_at = clock_timestamp(), finished_attempts = finished_attempts + 1 WHERE id = $1;',
          )
        ) {
          // markMessageAbandoned
          return { rowCount: 0 };
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

    markMessageCompletedSpy.mockReset();
    increaseMessageFinishedAttemptsSpy.mockReset();
  });
  afterEach(async () => {
    if (afterCleanup) {
      await afterCleanup();
    }
  });

  it('should call the correct messageHandler and acknowledge the WAL message when no errors are thrown', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message: StoredTransactionalMessage = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      concurrency: 'sequential',
      lockedUntil: '1970-01-01T00:00:00.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
      abandonedAt: null,
    };
    const [cleanup] = initializeReplicationMessageListener(
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
    expect(markMessageCompletedSpy).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
      expect.any(Object),
    );
    expect(increaseMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    expect(client.end).toHaveBeenCalledTimes(0);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should return an error if more than one messageHandler is registered for one aggregate/message type combination', () => {
    // Arrange
    const aggregateType = 'aggregate_type';
    const messageType = 'message_type';

    // Act + Assert
    expect(() =>
      initializeReplicationMessageListener(
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

  it('should not call a messageHandler but acknowledge the WAL message when the DB message was already process.', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

    // Act
    sendReplicationChunk('processed_id');
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    // As the DB message was already processed --> no marking as completed needed
    expect(markMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
  });

  it('should not call a messageHandler but acknowledge the WAL message when the DB message was somehow process by another process after the started attempts increment.', async () => {
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
            abandoned_at: null,
          },
        ],
      })
      // the call in initiateMessageProcessing
      .mockReturnValueOnce({
        rowCount: 1,
        rows: [
          {
            started_attempts: 1,
            finished_attempts: 0,
            processed_at: new Date(),
            abandoned_at: null,
          },
        ],
      });
    const messageHandler = jest.fn(() => Promise.resolve());
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

    // Act
    sendReplicationChunk('processed_id');
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    // As the DB message was already processed --> no marking as completed needed
    expect(markMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
  });

  it('should not call a messageHandler and not acknowledge the WAL message when the DB message was not found.', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

    // Act
    sendReplicationChunk('not_found' as MessageIdType);
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalled();
    // As the DB message was not found --> no marking as completed needed
    expect(markMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
  });

  it('should call a messageHandler on a not processed message but not acknowledge the WAL message when the message handler throws an error', async () => {
    // Arrange
    const handleError = jest.fn();
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const unusedErrorHandler = jest.fn();
    const message: StoredTransactionalMessage = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      concurrency: 'sequential',
      lockedUntil: '1970-01-01T00:00:00.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
      abandonedAt: null,
    };
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

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
    expect(markMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseMessageFinishedAttemptsSpy).toHaveBeenCalledWith(
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
    expect(client.end).toHaveBeenCalledTimes(1); // as part of error handling
  });

  it('on a message handler timeout the client should do a ROLLBACK and not commit', async () => {
    // Arrange
    const handleError = jest.fn();
    const message: StoredTransactionalMessage = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      concurrency: 'sequential',
      lockedUntil: '1970-01-01T00:00:00.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
      abandonedAt: null,
    };
    const [cleanup] = initializeReplicationMessageListener(
      config,
      [
        {
          aggregateType: message.aggregateType,
          messageType: message.messageType,
          handle: () => sleep(500),
          handleError,
        },
      ],
      getDisabledLogger(),
      {
        messageProcessingTimeoutStrategy: () => 100,
      },
    );
    afterCleanup = cleanup;

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    await sleep(200);
    const expectedMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
      finishedAttempts: message.finishedAttempts + 1,
    };
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalledWith();
    expect(markMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseMessageFinishedAttemptsSpy).toHaveBeenCalledWith(
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
    expect(client.end).toHaveBeenCalledTimes(1); // as part of error handling
  });

  it('should call the messageHandler and acknowledge the WAL message even if the started attempts are much higher than the finished when poisonous message protection is disabled', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const message: StoredTransactionalMessage = {
      id: 'poisonous_message_exceeded',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      concurrency: 'sequential',
      lockedUntil: '1970-01-01T00:00:00.000Z',
      startedAttempts: 99, // would be a poisonous message
      finishedAttempts: 0,
      processedAt: null,
      abandonedAt: null,
    };
    const cfg: ReplicationListenerConfig = {
      outboxOrInbox: 'inbox',
      dbHandlerConfig: {
        ...config.dbHandlerConfig,
      },
      dbListenerConfig: {
        ...config.dbListenerConfig,
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
          abandoned_at: null,
          locked_until: new Date('1970-01-01T00:00:00.000Z'),
        },
      ],
    });
    const strategies: Partial<ReplicationMessageStrategies> = {
      poisonousMessageRetryStrategy: jest.fn().mockReturnValue(false), // always treat it as poisonous
    };
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

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
    expect(markMessageCompletedSpy).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
      expect.any(Object),
    );
    expect(increaseMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
  });

  it('should not call the messageHandler on a poisonous message that already has exceeded the maximum poisonous retries', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const message: StoredTransactionalMessage = {
      id: 'poisonous_message_exceeded',
      aggregateType: aggregate_type,
      messageType: message_type,
      aggregateId: 'test_aggregate_id',
      concurrency: 'sequential',
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 4,
      finishedAttempts: 1,
      lockedUntil: '1970-01-01T00:00:00.000Z',
      processedAt: null,
      abandonedAt: null,
    };
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

    // Act
    sendReplicationChunk('poisonous_message_exceeded');
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    expect(markMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
  });

  it('should not retry a message when there is an error and the attempts are exceeded', async () => {
    // Arrange
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message: StoredTransactionalMessage = {
      id: 'last_attempt',
      aggregateType: aggregate_type,
      messageType: message_type,
      aggregateId: 'test_aggregate_id',
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      concurrency: 'sequential',
      lockedUntil: '1970-01-01T00:00:00.000Z',
      startedAttempts: 4,
      finishedAttempts: 4,
      processedAt: null,
      abandonedAt: null,
    };
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

    // Act
    sendReplicationChunk('last_attempt');
    await continueEventLoop();

    // Assert
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalledWith();
    expect(markMessageCompletedSpy).not.toHaveBeenCalled();
    expect(markMessageAbandonedSpy).toHaveBeenCalledWith(
      {
        ...message,
        finishedAttempts: message.finishedAttempts + 1,
        startedAttempts: message.startedAttempts + 1,
      },
      expect.any(Object),
      expect.any(Object),
    );
    expect(client.connect).toHaveBeenCalledTimes(1);
  });

  it('a messageHandler throws an error and the error handler throws an error as well the message should still increase attempts', async () => {
    // Arrange
    const handleError = jest.fn().mockImplementationOnce(() => {
      throw new Error('Error handling error');
    });
    const message: StoredTransactionalMessage = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      concurrency: 'sequential',
      lockedUntil: '1970-01-01T00:00:00.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
      abandonedAt: null,
    };
    const [logger, logs] = getInMemoryLogger('unit test');
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

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
    expect(markMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseMessageFinishedAttemptsSpy).toHaveBeenCalledWith(
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
        'The error handling of the inbox message failed. Please make sure that your error handling code does not throw an error! Attempting to retry the message.',
    );
    expect(log).toHaveLength(1);
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
    const message: StoredTransactionalMessage = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
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
    const [logger, logs] = getInMemoryLogger('unit test');
    const [cleanup] = initializeReplicationMessageListener(
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
            .mockReturnValueOnce(dbClientStrategy.getClient(message)) // initiateMessageProcessing
            .mockReturnValueOnce(dbClientStrategy.getClient(message)) // handleError
            .mockReturnValueOnce(null), // best effort increment - which should fail
          shutdown: dbClientStrategy.shutdown,
        },
      },
    );
    afterCleanup = cleanup;
    increaseMessageFinishedAttemptsSpy.mockRejectedValueOnce(
      new Error('Finished Attempts Increase'),
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
    expect(markMessageCompletedSpy).not.toHaveBeenCalled();
    expect(increaseMessageFinishedAttemptsSpy).toHaveBeenCalledTimes(1);
    expect(increaseMessageFinishedAttemptsSpy).toHaveBeenCalledWith(
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
        'The error handling of the inbox message failed. Please make sure that your error handling code does not throw an error! Attempting to retry the message.',
    );
    expect(log).toHaveLength(1);
    const bestEffortLog = logs.filter(
      (log) =>
        log.args[1] ===
        "The 'best-effort' logic to increase the inbox message finished attempts failed as well. Attempting to abandon the message.",
    );
    expect(bestEffortLog).toHaveLength(1);
  });

  it('should log a debug message when no messageHandler was found for a message', async () => {
    // Arrange
    const [logger, logs] = getInMemoryLogger('unit test');
    const messageHandler = jest.fn(() => Promise.resolve());
    const message: TransactionalMessage = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      concurrency: 'sequential',
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      lockedUntil: '1970-01-01T00:00:00.000Z',
      createdAt: '2023-01-18T21:02:27.000Z',
    };
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    expect(markMessageCompletedSpy).toHaveBeenCalledWith(
      message,
      expect.any(Object),
      expect.any(Object),
    );
    expect(increaseMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    const log = logs.filter(
      (log) =>
        log.args[0] ===
        'No inbox message handler found for aggregate type "test_type" and message tye "test_message_type"',
    );
    expect(log).toHaveLength(1);
    expect(client.connect).toHaveBeenCalledTimes(1);
  });

  it('should raise an error if no message handlers are defined', () => {
    // Act
    expect(() =>
      initializeReplicationMessageListener(config, [], getDisabledLogger()),
    ).toThrow('At least one message handler must be provided');
  });

  it('should use all the strategies', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message: StoredTransactionalMessage = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      messageType: message_type,
      payload: { result: 'success' },
      concurrency: 'sequential',
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
      abandonedAt: null,
      lockedUntil: '1970-01-01T00:00:00.000Z',
    };
    const strategies: ReplicationMessageStrategies = {
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
    const [cleanup] = initializeReplicationMessageListener(
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
    afterCleanup = cleanup;

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    const expectedMessage: StoredTransactionalMessage = {
      ...message,
      startedAttempts: message.startedAttempts + 1,
      concurrency: 'sequential', // default
      lockedUntil: '1970-01-01T00:00:00.000Z',
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
    expect(strategies.messageRetryStrategy).not.toHaveBeenCalled();
    expect(strategies.poisonousMessageRetryStrategy).not.toHaveBeenCalled();
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    expect(markMessageCompletedSpy).toHaveBeenCalledWith(
      expectedMessage,
      expect.any(Object),
      expect.any(Object),
    );
    expect(increaseMessageFinishedAttemptsSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
  });
});

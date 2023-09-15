/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Client, Connection, Pool, PoolClient } from 'pg';
import { Pgoutput } from 'pg-logical-replication';
import { InboxServiceConfig, initializeInboxService } from './inbox-service';
import { disableLogger } from './logger';
import * as inboxSpy from './inbox';
import EventEmitter from 'events';
import { sleep } from './utils';

type MessageIdType = 'not_processed_id' | 'processed_id' | 'retries_exceeded';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  disableLogger(); // Hide logs if the tests are not run in debug mode
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
            new: inboxDbMessageByFlag(buffer),
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
    case 'retries_exceeded':
      data[25] = 2;
      break;
  }
  const chunk = Buffer.from(data);
  (client as any).connection.emit('copyData', {
    chunk,
    length: chunk.length,
    name: 'copyData',
  });
};

const ackInboxSpy = jest.spyOn(inboxSpy, 'ackInbox');
const nackInboxSpy = jest.spyOn(inboxSpy, 'nackInbox');

jest.mock('./utils', () => {
  return {
    ...jest.requireActual('./utils'),
    executeTransaction: jest.fn(
      async (
        pool: Pool,
        callback: (client: PoolClient) => Promise<unknown>,
      ) => {
        const client = await pool.connect();
        const response = await callback(client);
        client.release();
        return response;
      },
    ),
  };
});

const aggregate_type = 'test_type';
const event_type = 'test_event_type';
const inboxDbMessages = [
  {
    id: 'not_processed_id' as MessageIdType,
    aggregate_type,
    event_type,
    aggregate_id: 'test_aggregate_id',
    payload: { result: 'success' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    retries: 2,
    processed_at: null,
  },
  {
    id: 'processed_id' as MessageIdType,
    aggregate_type,
    event_type,
    aggregate_id: 'test_aggregate_id',
    payload: { result: 'success' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    retries: 0,
    processed_at: new Date('2023-01-18T21:02:27.000Z'),
  },
  {
    id: 'retries_exceeded' as MessageIdType,
    aggregate_type,
    event_type,
    aggregate_id: 'test_aggregate_id',
    payload: { result: 'success' },
    created_at: new Date('2023-01-18T21:02:27.000Z'),
    retries: 4, // 5 is max by default
    processed_at: null,
  },
];
const inboxDbMessageById = (id: MessageIdType) =>
  inboxDbMessages.find((m) => m.id === id);

const inboxDbMessageByFlag = (buffer: Buffer) => {
  switch (buffer[0]) {
    case 0:
      return inboxDbMessageById('processed_id');
    case 1:
      return inboxDbMessageById('not_processed_id');
    case 2:
      return inboxDbMessageById('retries_exceeded');
  }
};

const config: InboxServiceConfig = {
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
        switch (sql) {
          case 'SELECT processed_at, retries FROM test_schema.test_table WHERE id = $1 FOR UPDATE NOWAIT': {
            const dbMessage = inboxDbMessageById(params[0] as MessageIdType);
            return {
              rowCount: dbMessage ? 1 : 0,
              rows: dbMessage ? [{ processed_at: dbMessage.processed_at }] : [],
            };
          }
        }
      }),
      connect: jest.fn(),
      connection,
      removeAllListeners: jest.fn(),
      on: jest.fn(),
      end: jest.fn(),
      release: jest.fn(),
    } as unknown as ReplicationClient;

    ackInboxSpy.mockReset();
    nackInboxSpy.mockReset();
  });

  it('should call the correct messageHandler and acknowledge the WAL message when no errors are thrown', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      eventType: event_type,
      payload: { result: 'success' },
      createdAt: '2023-01-18T21:02:27.000Z',
      retries: 2,
    };
    const [cleanup] = await initializeInboxService(config, [
      {
        aggregateType: message.aggregateType,
        eventType: message.eventType,
        handle: messageHandler,
      },
      {
        aggregateType: 'unused-aggregate-type',
        eventType: 'unused-event',
        handle: unusedMessageHandler,
      },
    ]);

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    expect(messageHandler).toHaveBeenCalledWith(message, expect.any(Object));
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    expect(ackInboxSpy).toHaveBeenCalledWith(
      message,
      expect.any(Object),
      expect.any(Object),
    );
    expect(nackInboxSpy).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should call all the correct messageHandlers and acknowledge the WAL message when no errors are thrown', async () => {
    // Arrange
    const messageHandler1 = jest.fn(() => Promise.resolve());
    const messageHandler2 = jest.fn(() => Promise.resolve());
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      eventType: event_type,
      payload: { result: 'success' },
      createdAt: '2023-01-18T21:02:27.000Z',
      retries: 2,
    };
    const [cleanup] = await initializeInboxService(config, [
      {
        aggregateType: message.aggregateType,
        eventType: message.eventType,
        handle: messageHandler1,
      },
      {
        aggregateType: message.aggregateType,
        eventType: message.eventType,
        handle: messageHandler2,
      },
      {
        aggregateType: 'unused-aggregate-type',
        eventType: 'unused-event',
        handle: unusedMessageHandler,
      },
    ]);

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    expect(messageHandler1).toHaveBeenCalledWith(message, expect.any(Object));
    expect(messageHandler2).toHaveBeenCalledWith(message, expect.any(Object));
    expect(ackInboxSpy).toHaveBeenCalledWith(
      message,
      expect.any(Object),
      expect.any(Object),
    );
    expect(nackInboxSpy).not.toHaveBeenCalled();
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it.each(['processed_id' as MessageIdType, 'not_found' as MessageIdType])(
    'should not call a messageHandler but acknowledge the WAL message when the inbox DB message was already process or not found: %p',
    async (messageId) => {
      // Arrange
      const messageHandler = jest.fn(() => Promise.resolve());
      const unusedMessageHandler = jest.fn(() => Promise.resolve());
      const message = {
        id: messageId,
        aggregateType: aggregate_type,
        aggregateId: 'test_aggregate_id',
        eventType: event_type,
        payload: { result: 'success' },
        createdAt: '2023-01-18T21:02:27.000Z',
        retries: 2,
      };
      const [cleanup] = await initializeInboxService(config, [
        {
          aggregateType: message.aggregateType,
          eventType: message.eventType,
          handle: messageHandler,
        },
        {
          aggregateType: 'unused-aggregate-type',
          eventType: 'unused-event',
          handle: unusedMessageHandler,
        },
      ]);

      // Act
      sendReplicationChunk(messageId);
      await continueEventLoop();

      // Assert
      expect(messageHandler).not.toHaveBeenCalled();
      expect(unusedMessageHandler).not.toHaveBeenCalled();
      expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
      // As the inbox DB message was not found or already processed --> no ack/nack needed
      expect(ackInboxSpy).not.toHaveBeenCalled();
      expect(nackInboxSpy).not.toHaveBeenCalled();
      expect(client.connect).toHaveBeenCalledTimes(1);
      await cleanup();
      expect(client.end).toHaveBeenCalledTimes(1);
    },
  );

  it('should call a messageHandler on a not processed message but not acknowledge the WAL message when the message handler throws an error', async () => {
    // Arrange
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message = {
      id: 'not_processed_id',
      aggregateType: aggregate_type,
      aggregateId: 'test_aggregate_id',
      eventType: event_type,
      payload: { result: 'success' },
      createdAt: '2023-01-18T21:02:27.000Z',
      retries: 2,
    };
    const [cleanup] = await initializeInboxService(config, [
      {
        aggregateType: message.aggregateType,
        eventType: message.eventType,
        handle: () => {
          throw new Error('Unit Test');
        },
      },
      {
        aggregateType: 'unused-aggregate-type',
        eventType: 'unused-event',
        handle: unusedMessageHandler,
      },
    ]);

    // Act
    sendReplicationChunk('not_processed_id');
    await continueEventLoop();

    // Assert
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalledWith();
    expect(ackInboxSpy).not.toHaveBeenCalled();
    expect(nackInboxSpy).toHaveBeenCalledWith(
      message,
      expect.any(Object),
      expect.any(Object),
    );
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(2); // once as part of error handling - once via shutdown
  });

  it('should call a messageHandler on a message that has reached the retry limit and thus acknowledge the WAL message when the message handler throws an error', async () => {
    // Arrange
    const unusedMessageHandler = jest.fn(() => Promise.resolve());
    const message = {
      id: 'retries_exceeded',
      aggregateType: aggregate_type,
      eventType: event_type,
      aggregateId: 'test_aggregate_id',
      payload: { result: 'success' },
      createdAt: '2023-01-18T21:02:27.000Z',
      retries: 4,
    };
    const [cleanup] = await initializeInboxService(config, [
      {
        aggregateType: message.aggregateType,
        eventType: message.eventType,
        handle: () => {
          throw new Error('Unit Test');
        },
      },
      {
        aggregateType: 'unused-aggregate-type',
        eventType: 'unused-event',
        handle: unusedMessageHandler,
      },
    ]);

    // Act
    sendReplicationChunk('retries_exceeded');
    await continueEventLoop();

    // Assert
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalledWith();
    expect(ackInboxSpy).not.toHaveBeenCalled();
    expect(nackInboxSpy).toHaveBeenCalledWith(
      message,
      expect.any(Object),
      expect.any(Object),
    );
    expect(client.connect).toHaveBeenCalledTimes(1);
    await cleanup();
    expect(client.end).toHaveBeenCalledTimes(2); // once as part of error handling - once via shutdown
  });
});

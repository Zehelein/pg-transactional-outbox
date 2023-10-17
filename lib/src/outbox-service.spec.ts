/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Pgoutput } from 'pg-logical-replication';
import { OutboxServiceConfig, initializeOutboxService } from './outbox-service';
import { disableLogger } from './logger';
import { Client, Connection } from 'pg';
import { EventEmitter } from 'stream';
import { sleep } from './utils';

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
        parse: jest.fn().mockReturnValue({
          tag: 'insert',
          relation,
          new: outboxDbMessage,
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
    Client: jest.fn().mockImplementation(() => {
      return client;
    }),
  };
});

// Send a valid chunk that represents log message
const replicationChunk = Buffer.from([
  119, 0, 0, 0, 0, 93, 162, 168, 0, 0, 0, 0, 0, 9, 162, 168, 0, 0, 2, 168, 74,
  108, 17, 127, 72, 66, 0, 0, 0, 0, 9, 162, 254, 96, 0, 2, 168, 74, 108, 17,
  119, 203, 0, 1, 233, 183,
]);
const sendReplicationChunk = (chunk = replicationChunk) => {
  (client as any).connection.emit('copyData', {
    chunk,
    length: chunk.length,
    name: 'copyData',
  });
};

const outboxDbMessage = {
  id: 'message_id',
  aggregate_type: 'test_type',
  message_type: 'test_message_type',
  aggregate_id: 'test_aggregate_id',
  payload: { result: 'success' },
  metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
  created_at: new Date('2023-01-18T21:02:27.000Z'),
};

const outboxMessage = {
  id: outboxDbMessage.id,
  aggregateType: outboxDbMessage.aggregate_type,
  aggregateId: outboxDbMessage.aggregate_id,
  messageType: outboxDbMessage.message_type,
  payload: outboxDbMessage.payload,
  metadata: outboxDbMessage.metadata,
  createdAt: '2023-01-18T21:02:27.000Z',
};

const config: OutboxServiceConfig = {
  pgReplicationConfig: {
    host: 'test_host',
    port: 5432,
    database: 'test_db',
    user: 'test_outbox_user',
    password: 'test_outbox_user_password',
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

describe('Outbox service unit tests - initializeOutboxService', () => {
  beforeEach(() => {
    const connection = new EventEmitter();
    (connection as any).sendCopyFromChunk = jest.fn();

    client = {
      query: jest.fn(),
      connect: jest.fn(),
      connection,
      removeAllListeners: jest.fn(),
      on: jest.fn(),
      end: jest.fn(),
    } as unknown as ReplicationClient;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should call the messageHandler and acknowledge the WAL message when no errors are thrown', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const [shutdown] = initializeOutboxService(config, messageHandler);
    await continueEventLoop();

    // Act
    sendReplicationChunk();
    await continueEventLoop();

    // Assert
    expect(messageHandler).toHaveBeenCalledWith(outboxMessage);
    expect(client.connection.sendCopyFromChunk).toHaveBeenCalledWith(
      expect.any(Buffer), // acknowledge
    );
    expect(client.connect).toHaveBeenCalledTimes(1);
    await shutdown();
    expect(client.end).toHaveBeenCalledTimes(1);
  });

  it('should call the messageHandler but not acknowledge the WAL message when the message handler throws an error', async () => {
    // Arrange
    const messageHandler = jest.fn(async () => {
      throw new Error('Unit Test');
    });
    const [shutdown] = initializeOutboxService(config, messageHandler);
    await continueEventLoop();

    // Act
    sendReplicationChunk();
    await continueEventLoop();

    // Assert
    expect(messageHandler).toHaveBeenCalledWith(outboxMessage);
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await shutdown();
    expect(client.end).toHaveBeenCalledTimes(2); // once as part of error handling - once via shutdown
  });

  it('should call acknowledge when receiving a keep alive message', async () => {
    // Arrange
    const messageHandler = jest.fn(async () => {
      throw new Error('Unit Test');
    });
    const [shutdown] = initializeOutboxService(config, messageHandler);
    await continueEventLoop();
    const keepAliveChunk = Buffer.from([
      107, 0, 0, 0, 0, 9, 163, 25, 136, 0, 2, 168, 78, 55, 139, 118, 225, 0,
    ]);

    // Act
    sendReplicationChunk(keepAliveChunk);
    await continueEventLoop();

    // Assert
    expect(messageHandler).not.toHaveBeenCalledWith(outboxMessage);
    expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalled();
    expect(client.connect).toHaveBeenCalledTimes(1);
    await shutdown();
    expect(client.end).toHaveBeenCalledTimes(1);
  });
});

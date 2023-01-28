/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Client, Pool, PoolClient } from 'pg';
import { Pgoutput } from 'pg-logical-replication';
import { InboxServiceConfig, initializeInboxService } from './inbox-service';
import { disableLogger } from './logger';
import * as inboxSpy from './inbox';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  disableLogger(); // Hide logs if the tests are not run in debug mode
  jest.setTimeout(7_000);
}

const ackInboxSpy = jest.spyOn(inboxSpy, 'ackInbox');
const nackInboxSpy = jest.spyOn(inboxSpy, 'nackInbox');

const repService: {
  // We expose the callback methods from the underlying LogicalReplicationService
  // that are normally called from the "on" "data/error" emitted events to be
  // able to manually call them. This bypasses also the service error
  // catching/restart logic which helps to run the test only once.
  handleData?: (lsn: string, log: any) => Promise<void> | void;
  handleError?: (err: Error) => void;
  acknowledge?: jest.Mock<any, any>;
  stop?: jest.Mock<any, any>;
} = {};
jest.mock('pg-logical-replication', () => {
  return {
    ...jest.requireActual('pg-logical-replication'),
    LogicalReplicationService: jest.fn().mockImplementation(() => {
      const lrs = {
        // on: jest.fn(),
        handleData: undefined,
        handleError: undefined,
        on: (event: 'data' | 'error', listener: any) => {
          switch (event) {
            case 'data':
              repService.handleData = listener;
              break;
            case 'error':
              repService.handleError = listener;
              break;
          }
        },
        acknowledge: jest.fn(),
        removeAllListeners: jest.fn(),
        emit: jest.fn(),
        stop: jest.fn(() => Promise.resolve()),
        subscribe: () =>
          new Promise(() => {
            /** never return */
          }),
        isStop: () => true,
      };
      repService.acknowledge = lrs.acknowledge;
      repService.stop = lrs.stop;
      return lrs;
    }),
  };
});

jest.mock('pg', () => {
  return {
    ...jest.requireActual('pg'),
    Pool: jest.fn().mockImplementation(() => ({
      connect: jest.fn(() => new Client()),
      on: jest.fn(),
      end: jest.fn(() => Promise.resolve()),
      removeAllListeners: jest.fn(),
    })),
    Client: jest.fn().mockImplementation(() => ({
      query: jest.fn(async (sql: string, params: [any]) => {
        switch (sql) {
          case 'SELECT processed_at FROM test_schema.test_table WHERE id = $1 FOR UPDATE NOWAIT': {
            const dbMessage = inboxDbMessage(params[0] as string);
            return {
              rowCount: dbMessage ? 1 : 0,
              rows: dbMessage ? [{ processed_at: dbMessage.processed_at }] : [],
            };
          }
        }
      }),
      on: jest.fn(),
      release: jest.fn(),
    })),
  };
});

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
const inboxDbMessage = (id: string) =>
  [
    {
      id: 'not_processed_id',
      aggregate_type,
      event_type,
      aggregate_id: 'test_aggregate_id',
      payload: { result: 'success' },
      created_at: new Date('2023-01-18T21:02:27.000Z'),
      retries: 2,
      processed_at: null,
    },
    {
      id: 'processed_id',
      aggregate_type,
      event_type,
      aggregate_id: 'test_aggregate_id',
      payload: { result: 'success' },
      created_at: new Date('2023-01-18T21:02:27.000Z'),
      retries: 0,
      processed_at: new Date('2023-01-18T21:02:27.000Z'),
    },
    {
      id: 'retries_exceeded',
      aggregate_type,
      event_type,
      aggregate_id: 'test_aggregate_id',
      payload: { result: 'success' },
      created_at: new Date('2023-01-18T21:02:27.000Z'),
      retries: 4, // 5 is max by default
      processed_at: null,
    },
  ].find((m) => m.id === id);

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
    repService.handleData = undefined;
    repService.handleError = undefined;
    repService.acknowledge = undefined;
    repService.stop = undefined;
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
    expect(repService.handleData).toBeDefined();
    expect(repService.handleError).toBeDefined();

    // Act
    await repService.handleData!('0/00000001', {
      tag: 'insert',
      relation,
      new: inboxDbMessage('not_processed_id'),
    });

    // Assert
    expect(messageHandler).toHaveBeenCalledWith(message, expect.any(Object));
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(repService.acknowledge).toHaveBeenCalledWith('0/00000001');
    expect(ackInboxSpy).toHaveBeenCalledWith(
      message,
      expect.any(Object),
      expect.any(Object),
    );
    expect(nackInboxSpy).not.toHaveBeenCalled();
    expect(repService.stop).not.toHaveBeenCalled();
    await cleanup();
    expect(repService.stop).toHaveBeenCalledTimes(1);
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
    expect(repService.handleData).toBeDefined();
    expect(repService.handleError).toBeDefined();

    // Act
    await repService.handleData!('0/00000001', {
      tag: 'insert',
      relation,
      new: inboxDbMessage('not_processed_id'),
    });

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
    expect(repService.acknowledge).toHaveBeenCalledWith('0/00000001');
    expect(repService.stop).not.toHaveBeenCalled();
    await cleanup();
    expect(repService.stop).toHaveBeenCalledTimes(1);
  });

  it.each(['processed_id', 'not_found'])(
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
      expect(repService.handleData).toBeDefined();
      expect(repService.handleError).toBeDefined();

      // Act
      await repService.handleData!('0/00000001', {
        tag: 'insert',
        relation,
        new: inboxDbMessage('processed_id'),
      });

      // Assert
      expect(messageHandler).not.toHaveBeenCalled();
      expect(unusedMessageHandler).not.toHaveBeenCalled();
      expect(repService.acknowledge).toHaveBeenCalledWith('0/00000001');
      // As the inbox DB message was not found or already processed --> no ack/nack needed
      expect(ackInboxSpy).not.toHaveBeenCalled();
      expect(nackInboxSpy).not.toHaveBeenCalled();
      expect(repService.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(repService.stop).toHaveBeenCalledTimes(1);
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
        handle: async () => {
          throw new Error('Unit Test');
        },
      },
      {
        aggregateType: 'unused-aggregate-type',
        eventType: 'unused-event',
        handle: unusedMessageHandler,
      },
    ]);
    expect(repService.handleData).toBeDefined();
    expect(repService.handleError).toBeDefined();

    // Act
    await repService.handleData!('0/00000001', {
      tag: 'insert',
      relation,
      new: inboxDbMessage('not_processed_id'),
    });

    // Assert
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(repService.acknowledge).not.toHaveBeenCalled();
    expect(ackInboxSpy).not.toHaveBeenCalled();
    expect(nackInboxSpy).toHaveBeenCalledWith(
      message,
      expect.any(Object),
      expect.any(Object),
    );
    expect(repService.stop).not.toHaveBeenCalled();
    await cleanup();
    expect(repService.stop).toHaveBeenCalledTimes(1);
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
        handle: async () => {
          throw new Error('Unit Test');
        },
      },
      {
        aggregateType: 'unused-aggregate-type',
        eventType: 'unused-event',
        handle: unusedMessageHandler,
      },
    ]);
    expect(repService.handleData).toBeDefined();
    expect(repService.handleError).toBeDefined();

    // Act
    await repService.handleData!('0/00000001', {
      tag: 'insert',
      relation,
      new: inboxDbMessage('retries_exceeded'),
    });

    // Assert
    expect(unusedMessageHandler).not.toHaveBeenCalled();
    expect(repService.acknowledge).not.toHaveBeenCalled();
    expect(ackInboxSpy).not.toHaveBeenCalled();
    expect(nackInboxSpy).toHaveBeenCalledWith(
      message,
      expect.any(Object),
      expect.any(Object),
    );
    expect(repService.stop).not.toHaveBeenCalled();
    await cleanup();
    expect(repService.stop).toHaveBeenCalledTimes(1);
  });
});

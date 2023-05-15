/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Pgoutput } from 'pg-logical-replication';
import { OutboxServiceConfig, initializeOutboxService } from './outbox-service';
import { disableLogger } from './logger';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  disableLogger(); // Hide logs if the tests are not run in debug mode
  jest.setTimeout(7_000);
}

const repService: {
  // We expose the callback methods from the underlying LogicalReplicationService
  // that are normally called from the "on" "data/error/heartbeat" emitted events
  // to be able to manually call them. This bypasses also the service error
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
        isStop: () => false,
      };
      repService.acknowledge = lrs.acknowledge;
      repService.stop = lrs.stop;
      return lrs;
    }),
  };
});

const outboxDbMessage = {
  id: 'message_id',
  aggregate_type: 'test_type',
  event_type: 'test_event_type',
  aggregate_id: 'test_aggregate_id',
  payload: { result: 'success' },
  created_at: new Date('2023-01-18T21:02:27.000Z'),
};

const outboxMessage = {
  id: outboxDbMessage.id,
  aggregateType: outboxDbMessage.aggregate_type,
  aggregateId: outboxDbMessage.aggregate_id,
  eventType: outboxDbMessage.event_type,
  payload: outboxDbMessage.payload,
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
    repService.handleData = undefined;
    repService.handleError = undefined;
    repService.acknowledge = undefined;
    repService.stop = undefined;
  });

  it('should call the messageHandler and acknowledge the WAL message when no errors are thrown', async () => {
    // Arrange
    const messageHandler = jest.fn(() => Promise.resolve());
    const [cleanup] = await initializeOutboxService(config, messageHandler);
    expect(repService.handleData).toBeDefined();
    expect(repService.handleError).toBeDefined();

    // Act
    await repService.handleData!('0/00000001', {
      tag: 'insert',
      relation,
      new: outboxDbMessage,
    });

    // Assert
    expect(messageHandler).toHaveBeenCalledWith(outboxMessage);
    expect(repService.acknowledge).toHaveBeenCalledWith('0/00000001');
    expect(repService.stop).not.toHaveBeenCalled();
    await cleanup();
    expect(repService.stop).toHaveBeenCalledTimes(1);
  });

  it('should call the messageHandler but not acknowledge the WAL message when the message handler throws an error', async () => {
    // Arrange
    const throwErrorMessageHandler = jest.fn(async () => {
      throw new Error('Unit Test');
    });
    const [cleanup] = await initializeOutboxService(
      config,
      throwErrorMessageHandler,
    );
    expect(repService.handleData).toBeDefined();
    expect(repService.handleError).toBeDefined();

    // Act
    await repService.handleData!('0/00000001', {
      tag: 'insert',
      relation,
      new: outboxDbMessage,
    });

    // Assert
    expect(throwErrorMessageHandler).toHaveBeenCalledWith(outboxMessage);
    expect(repService.acknowledge).not.toHaveBeenCalled();
    expect(repService.stop).not.toHaveBeenCalled();
    await cleanup();
    expect(repService.stop).toHaveBeenCalledTimes(1);
  });
});

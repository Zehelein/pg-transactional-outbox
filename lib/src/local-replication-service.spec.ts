/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Pgoutput } from 'pg-logical-replication';
import {
  createService,
  OutboxMessage,
  __only_for_unit_tests__ as tests,
} from './local-replication-service';
import { sleep } from './utils';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000); // for the 5sec heartbeat
}

const listeners: {
  handleData?: (lsn: string, log: any) => Promise<void> | void;
  handleError?: (err: Error) => void;
  handleHeartbeat?: (
    lsn: string,
    timestamp: number,
    shouldRespond: boolean,
  ) => Promise<void> | void;
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
        handleHeartbeat: undefined,
        on: (event: 'data' | 'error' | 'heartbeat', listener: any) => {
          switch (event) {
            case 'data':
              listeners.handleData = listener;
              break;
            case 'error':
              listeners.handleError = listener;
              break;
            case 'heartbeat':
              listeners.handleHeartbeat = listener;
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
      listeners.acknowledge = lrs.acknowledge;
      listeners.stop = lrs.stop;
      return lrs;
    }),
  };
});

const settings = {
  dbSchema: 'test_schema',
  dbTable: 'test_table',
  postgresPub: 'test_pub',
  postgresSlot: 'test_slot',
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
    {
      name: 'name',
      flags: 0,
      typeOid: 25,
      typeMod: -1,
      typeSchema: 'pg_catalog',
      typeName: 'text',
      parser: (raw: any) => raw,
    },
  ],
  keyColumns: ['id'],
};

describe('Local replication service unit tests', () => {
  describe('getRelevantMessage', () => {
    const mapAdditionalRows = jest.fn();
    it('should return undefined if log tag is not "insert"', () => {
      const update: Pgoutput.MessageUpdate = {
        tag: 'update',
        relation: relation,
        key: { id: 1 },
        old: { id: 1, name: 'old_name' },
        new: { id: 1, name: 'new_name' },
      };
      expect(tests?.getRelevantMessage?.(update, settings)).toBeUndefined();
    });

    it('should return undefined if log relation schema is not the same as settings', () => {
      const log: Pgoutput.MessageInsert = {
        tag: 'insert',
        relation: {
          ...relation,
          schema: 'wrong_schema',
        },
        new: {},
      };
      expect(tests?.getRelevantMessage?.(log, settings)).toBeUndefined();
    });

    it('should return undefined if log relation name is not the same as settings', () => {
      const log: Pgoutput.MessageInsert = {
        tag: 'insert',
        relation: {
          ...relation,
          name: 'wrong_name',
        },
        new: {},
      };
      expect(tests?.getRelevantMessage?.(log, settings)).toBeUndefined();
    });

    it('should return the outbox message if log relation schema and name are the same as settings', () => {
      // Arrange
      const log: Pgoutput.MessageInsert = {
        tag: 'insert',
        relation,
        new: {
          id: 'test_id',
          aggregate_type: 'test_type',
          aggregate_id: 'test_aggregate_id',
          event_type: 'test_event_type',
          payload: { test: 'payload' },
          created_at: new Date('2023-01-18T21:02:27.000Z'),
        },
      };
      // Act
      const message = tests?.getRelevantMessage?.(
        log,
        settings,
        mapAdditionalRows,
      );
      // Assert
      expect(message).toEqual({
        id: 'test_id',
        aggregateType: 'test_type',
        aggregateId: 'test_aggregate_id',
        eventType: 'test_event_type',
        payload: { test: 'payload' },
        createdAt: '2023-01-18T21:02:27.000Z',
      });
      expect(mapAdditionalRows).toHaveBeenCalledWith({
        id: 'test_id',
        aggregate_type: 'test_type',
        aggregate_id: 'test_aggregate_id',
        event_type: 'test_event_type',
        payload: { test: 'payload' },
        created_at: new Date('2023-01-18T21:02:27.000Z'),
      });
    });
  });

  describe('getOutboxMessage', () => {
    it('should return undefined if input is not an object', () => {
      expect(tests?.getOutboxMessage?.('not an object')).toBeUndefined();
      expect(tests?.getOutboxMessage?.(null)).toBeUndefined();
      expect(tests?.getOutboxMessage?.(undefined)).toBeUndefined();
    });

    it('should return undefined if input is missing required properties', () => {
      expect(tests?.getOutboxMessage?.({})).toBeUndefined();
      expect(
        tests?.getOutboxMessage?.({
          id: '123',
          aggregate_type: 'test_type',
        }),
      ).toBeUndefined();
    });

    it('should return undefined if input has wrong types for required properties', () => {
      expect(
        tests?.getOutboxMessage?.({
          id: 123, // not a string
          aggregate_type: 'test_type',
          aggregate_id: 'test_id',
          event_type: 'test_event',
          created_at: 'not a date',
          payload: { test: 'payload' },
        }),
      ).toBeUndefined();
    });

    it('should return an OutboxMessage if input is valid', () => {
      const input = {
        id: '123',
        aggregate_type: 'test_type',
        aggregate_id: 'test_id',
        event_type: 'test_event',
        created_at: new Date('2023-01-18T21:02:27.000Z'),
        payload: { test: 'payload' },
      };
      const message = tests?.getOutboxMessage?.(input);
      expect(message).toBeDefined();
      expect(message).toEqual({
        id: input.id,
        aggregateType: input.aggregate_type,
        aggregateId: input.aggregate_id,
        eventType: input.event_type,
        payload: input.payload,
        createdAt: '2023-01-18T21:02:27.000Z',
      });
    });

    it('should map additional properties if mapAdditionalRows is provided', () => {
      const input = {
        id: '123',
        aggregate_type: 'test_type',
        aggregate_id: 'test_id',
        event_type: 'test_event',
        created_at: new Date('2023-01-18T21:02:27.000Z'),
        payload: { test: 'payload' },
        additional_property: 'additional_value',
      };
      const mapAdditionalRows = (row: object) => ({
        additional_property: (row as typeof input).additional_property,
      });
      const message = tests?.getOutboxMessage?.(input, mapAdditionalRows);
      expect(message).toBeDefined();
      expect(message).toEqual({
        id: '123',
        aggregateType: 'test_type',
        aggregateId: 'test_id',
        eventType: 'test_event',
        payload: { test: 'payload' },
        createdAt: '2023-01-18T21:02:27.000Z',
        additional_property: 'additional_value',
      });
    });
  });

  describe('createService', () => {
    let messageHandler: jest.Mock;
    let errorHandler: jest.Mock;
    let mapAdditionalRows: jest.Mock;

    beforeEach(() => {
      messageHandler = jest.fn();
      errorHandler = jest.fn();
      mapAdditionalRows = jest.fn();

      listeners.handleData = undefined;
      listeners.handleError = undefined;
      listeners.handleHeartbeat = undefined;
      listeners.acknowledge = undefined;
      listeners.stop = undefined;
    });

    it('should call messageHandler and acknowledge the message when no errors are thrown', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const [cleanup] = await createService<OutboxMessage>(
        config,
        messageHandler,
        errorHandler,
        mapAdditionalRows,
      );
      const message = {
        id: 'test_id',
        aggregateType: 'test_type',
        aggregateId: 'test_aggregate_id',
        eventType: 'test_event_type',
        payload: { result: 'success' },
        createdAt: '2023-01-18T21:02:27.000Z',
      };
      expect(listeners.handleData).toBeDefined();

      // Act
      await listeners.handleData!('0/00000001', {
        tag: 'insert',
        relation,
        new: {
          id: 'test_id',
          aggregate_type: 'test_type',
          aggregate_id: 'test_aggregate_id',
          event_type: 'test_event_type',
          payload: { result: 'success' },
          created_at: new Date('2023-01-18T21:02:27.000Z'),
        },
      });

      // Assert
      expect(listeners.handleError).toBeDefined();
      expect(listeners.handleHeartbeat).toBeDefined();
      expect(messageHandler).toHaveBeenCalledWith(message);
      expect(errorHandler).not.toHaveBeenCalled();
      expect(mapAdditionalRows).toHaveBeenCalled();
      expect(listeners.acknowledge).toHaveBeenCalledWith('0/00000001');
      expect(listeners.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(listeners.stop).toHaveBeenCalledTimes(1);
    });

    it('should call messageHandler but not acknowledge the message when an error is thrown', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const testError = new Error('Transient error');
      let errorHandlerCalled = false;
      const [cleanup] = await createService<OutboxMessage>(
        config,
        async () => {
          throw testError;
        },
        async (error, msg, _ack) => {
          expect(error).toStrictEqual(testError);
          expect(msg).toStrictEqual(message);
          errorHandlerCalled = true;
          return true;
        },
        mapAdditionalRows,
      );
      const message = {
        id: 'test_id',
        aggregateType: 'test_type',
        aggregateId: 'test_aggregate_id',
        eventType: 'test_event_type',
        payload: { result: 'success' },
        createdAt: '2023-01-18T21:02:27.000Z',
      };
      expect(listeners.handleData).toBeDefined();

      // Act
      await listeners.handleData!('0/00000001', {
        tag: 'insert',
        relation,
        new: {
          id: 'test_id',
          aggregate_type: 'test_type',
          aggregate_id: 'test_aggregate_id',
          event_type: 'test_event_type',
          payload: { result: 'success' },
          created_at: new Date('2023-01-18T21:02:27.000Z'),
        },
      });

      // Assert
      expect(listeners.handleError).toBeDefined();
      expect(listeners.handleHeartbeat).toBeDefined();
      expect(errorHandlerCalled).toBe(true);
      expect(mapAdditionalRows).toHaveBeenCalled();
      expect(errorHandler).not.toHaveBeenCalled();
      expect(listeners.acknowledge).not.toHaveBeenCalled();
      expect(listeners.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(listeners.stop).toHaveBeenCalledTimes(1);
    });

    it('should call messageHandler and acknowledge the message when an error is thrown', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const testError = new Error('Non recoverable error');
      let errorHandlerCalled = false;
      const [cleanup] = await createService<OutboxMessage>(
        config,
        async () => {
          throw testError;
        },
        async (error, msg, ack) => {
          expect(error).toStrictEqual(testError);
          expect(msg).toStrictEqual(message);
          errorHandlerCalled = true;
          await ack();
          return true;
        },
        mapAdditionalRows,
      );
      const message = {
        id: 'test_id',
        aggregateType: 'test_type',
        aggregateId: 'test_aggregate_id',
        eventType: 'test_event_type',
        payload: { result: 'success' },
        createdAt: '2023-01-18T21:02:27.000Z',
      };
      expect(listeners.handleData).toBeDefined();

      // Act
      await listeners.handleData!('0/00000001', {
        tag: 'insert',
        relation,
        new: {
          id: 'test_id',
          aggregate_type: 'test_type',
          aggregate_id: 'test_aggregate_id',
          event_type: 'test_event_type',
          payload: { result: 'success' },
          created_at: new Date('2023-01-18T21:02:27.000Z'),
        },
      });

      // Assert
      expect(listeners.handleError).toBeDefined();
      expect(listeners.handleHeartbeat).toBeDefined();
      expect(errorHandlerCalled).toBe(true);
      expect(errorHandler).not.toHaveBeenCalled();
      expect(mapAdditionalRows).toHaveBeenCalled();
      expect(listeners.acknowledge).toHaveBeenCalled();
      expect(listeners.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(listeners.stop).toHaveBeenCalledTimes(1);
    });

    it('A heartbeat should be acknowledged after 5 seconds', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const [cleanup] = await createService<OutboxMessage>(
        config,
        messageHandler,
        errorHandler,
        mapAdditionalRows,
      );
      expect(listeners.handleHeartbeat).toBeDefined();

      // Act
      await listeners.handleHeartbeat!('0/00000001', 123, true);

      // Assert
      expect(listeners.handleData).toBeDefined();
      expect(listeners.handleError).toBeDefined();
      expect(messageHandler).not.toHaveBeenCalled();
      expect(mapAdditionalRows).not.toHaveBeenCalled();
      expect(errorHandler).not.toHaveBeenCalled();
      expect(listeners.acknowledge).not.toHaveBeenCalled();
      await sleep(4000);
      expect(listeners.acknowledge).not.toHaveBeenCalled();
      await sleep(1010);
      expect(listeners.acknowledge).toHaveBeenCalled();
      expect(listeners.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(listeners.stop).toHaveBeenCalledTimes(1);
    });

    it('A heartbeat should not be acknowledged after 5 seconds when a message acknowledgement comes in between', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const [cleanup] = await createService<OutboxMessage>(
        config,
        messageHandler,
        errorHandler,
        mapAdditionalRows,
      );
      expect(listeners.handleData).toBeDefined();
      expect(listeners.handleHeartbeat).toBeDefined();

      // Act
      await listeners.handleHeartbeat!('0/00000001', 123, true);
      await sleep(1000);
      await listeners.handleData!('0/00000002', {
        tag: 'insert',
        relation,
        new: {
          id: 'test_id',
          aggregate_type: 'test_type',
          aggregate_id: 'test_aggregate_id',
          event_type: 'test_event_type',
          payload: { result: 'success' },
          created_at: new Date('2023-01-18T21:02:27.000Z'),
        },
      });

      // Assert
      expect(listeners.handleError).toBeDefined();
      expect(messageHandler).toHaveBeenCalled();
      expect(mapAdditionalRows).toHaveBeenCalled();
      expect(errorHandler).not.toHaveBeenCalled();
      expect(listeners.acknowledge).toHaveBeenCalledWith('0/00000002');
      await sleep(4010);
      expect(listeners.acknowledge).toHaveBeenCalledTimes(1);
      expect(listeners.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(listeners.stop).toHaveBeenCalledTimes(1);
    });
  });
});

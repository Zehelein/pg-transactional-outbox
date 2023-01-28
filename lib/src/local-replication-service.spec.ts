/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Pgoutput } from 'pg-logical-replication';
import {
  createService,
  __only_for_unit_tests__ as tests,
} from './local-replication-service';
import { disableLogger } from './logger';
import { OutboxMessage } from './models';
import { sleep } from './utils';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000); // for the 5sec heartbeat
  disableLogger(); // Hide logs if the tests are not run in debug mode
}

const repService: {
  // We expose the callback methods that are normally called from the "on"
  // "data/error/heartbeat" emitted events to be able to manually call them.
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
        handleData: undefined,
        handleError: undefined,
        handleHeartbeat: undefined,
        on: (event: 'data' | 'error' | 'heartbeat', listener: any) => {
          switch (event) {
            case 'data':
              repService.handleData = listener;
              break;
            case 'error':
              repService.handleError = listener;
              break;
            case 'heartbeat':
              repService.handleHeartbeat = listener;
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
      // Arrange
      const update: Pgoutput.MessageUpdate = {
        tag: 'update',
        relation: relation,
        key: { id: 1 },
        old: { id: 1, name: 'old_name' },
        new: { id: 1, name: 'new_name' },
      };

      // Act
      const message = tests?.getRelevantMessage?.(update, settings);

      // Assert
      expect(message).toBeUndefined();
    });

    it('should return undefined if log relation schema is not the same as settings', () => {
      // Arrange
      const log: Pgoutput.MessageInsert = {
        tag: 'insert',
        relation: {
          ...relation,
          schema: 'wrong_schema',
        },
        new: {},
      };

      // Act
      const message = tests?.getRelevantMessage?.(log, settings);

      // Assert
      expect(message).toBeUndefined();
    });

    it('should return undefined if log relation name is not the same as settings', () => {
      // Arrange
      const log: Pgoutput.MessageInsert = {
        tag: 'insert',
        relation: {
          ...relation,
          name: 'wrong_name',
        },
        new: {},
      };

      // Act
      const message = tests?.getRelevantMessage?.(log, settings);

      // Assert
      expect(message).toBeUndefined();
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

  describe('mapMessage', () => {
    it('should return undefined if input is not an object', () => {
      // Act + Assert
      expect(tests?.mapMessage?.('not an object')).toBeUndefined();
      expect(tests?.mapMessage?.(null)).toBeUndefined();
      expect(tests?.mapMessage?.(undefined)).toBeUndefined();
    });

    it('should return undefined if input is missing required properties', () => {
      // Act + Assert
      expect(tests?.mapMessage?.({})).toBeUndefined();
      expect(
        tests?.mapMessage?.({
          id: '123',
          aggregate_type: 'test_type',
        }),
      ).toBeUndefined();
    });

    it('should return undefined if input has wrong types for required properties', () => {
      // Act + Assert
      expect(
        tests?.mapMessage?.({
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
      // Arrange
      const input = {
        id: '123',
        aggregate_type: 'test_type',
        aggregate_id: 'test_id',
        event_type: 'test_event',
        created_at: new Date('2023-01-18T21:02:27.000Z'),
        payload: { test: 'payload' },
      };

      // Act
      const message = tests?.mapMessage?.(input);

      // Assert
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
      // Arrange
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

      // Act
      const message = tests?.mapMessage?.(input, mapAdditionalRows);

      // Assert
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

      repService.handleData = undefined;
      repService.handleError = undefined;
      repService.handleHeartbeat = undefined;
      repService.acknowledge = undefined;
      repService.stop = undefined;
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
      expect(repService.handleData).toBeDefined();

      // Act
      await repService.handleData!('0/00000001', {
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
      expect(repService.handleError).toBeDefined();
      expect(repService.handleHeartbeat).toBeDefined();
      expect(messageHandler).toHaveBeenCalledWith(message);
      expect(errorHandler).not.toHaveBeenCalled();
      expect(mapAdditionalRows).toHaveBeenCalled();
      expect(repService.acknowledge).toHaveBeenCalledWith('0/00000001');
      expect(repService.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(repService.stop).toHaveBeenCalledTimes(1);
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
        async (msg, error) => {
          expect(msg).toStrictEqual(message);
          expect(error).toStrictEqual(testError);
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
      expect(repService.handleData).toBeDefined();

      // Act
      await repService.handleData!('0/00000001', {
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
      expect(repService.handleError).toBeDefined();
      expect(repService.handleHeartbeat).toBeDefined();
      expect(errorHandlerCalled).toBe(true);
      expect(mapAdditionalRows).toHaveBeenCalled();
      expect(errorHandler).not.toHaveBeenCalled();
      expect(repService.acknowledge).not.toHaveBeenCalled();
      expect(repService.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(repService.stop).toHaveBeenCalledTimes(1);
    });

    it('should call messageHandler and not acknowledge the message when an error is thrown', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const testError = new Error('Unit test error');
      let errorHandlerCalled = false;
      const [cleanup] = await createService<OutboxMessage>(
        config,
        async () => {
          throw testError;
        },
        async (msg, error) => {
          expect(msg).toStrictEqual(message);
          expect(error).toStrictEqual(testError);
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
      expect(repService.handleData).toBeDefined();

      // Act
      await repService.handleData!('0/00000001', {
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
      expect(repService.handleError).toBeDefined();
      expect(repService.handleHeartbeat).toBeDefined();
      expect(errorHandlerCalled).toBe(true);
      expect(errorHandler).not.toHaveBeenCalled();
      expect(mapAdditionalRows).toHaveBeenCalled();
      expect(repService.acknowledge).not.toHaveBeenCalled();
      expect(repService.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(repService.stop).toHaveBeenCalledTimes(1);
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
      expect(repService.handleHeartbeat).toBeDefined();

      // Act
      await repService.handleHeartbeat!('0/00000001', 123, true);

      // Assert
      expect(repService.handleData).toBeDefined();
      expect(repService.handleError).toBeDefined();
      expect(messageHandler).not.toHaveBeenCalled();
      expect(mapAdditionalRows).not.toHaveBeenCalled();
      expect(errorHandler).not.toHaveBeenCalled();
      expect(repService.acknowledge).not.toHaveBeenCalled();
      await sleep(4000);
      expect(repService.acknowledge).not.toHaveBeenCalled();
      await sleep(1010);
      expect(repService.acknowledge).toHaveBeenCalled();
      expect(repService.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(repService.stop).toHaveBeenCalledTimes(1);
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
      expect(repService.handleData).toBeDefined();
      expect(repService.handleHeartbeat).toBeDefined();

      // Act
      await repService.handleHeartbeat!('0/00000001', 123, true);
      await sleep(1000);
      await repService.handleData!('0/00000002', {
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
      expect(repService.handleError).toBeDefined();
      expect(messageHandler).toHaveBeenCalled();
      expect(mapAdditionalRows).toHaveBeenCalled();
      expect(errorHandler).not.toHaveBeenCalled();
      expect(repService.acknowledge).toHaveBeenCalledWith('0/00000002');
      await sleep(4010);
      expect(repService.acknowledge).toHaveBeenCalledTimes(1);
      expect(repService.stop).not.toHaveBeenCalled();
      await cleanup();
      expect(repService.stop).toHaveBeenCalledTimes(1);
    });
  });
});

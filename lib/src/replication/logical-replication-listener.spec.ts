/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-non-null-assertion */
import EventEmitter from 'events';
import inspector from 'inspector';
import { Client, Connection } from 'pg';
import { Pgoutput } from 'pg-logical-replication';
import { getDisabledLogger } from '../common/logger';
import { OutboxMessage } from '../common/message';
import { sleep } from '../common/utils';
import { defaultConcurrencyStrategy } from '../strategies/concurrency-strategy';
import { defaultMessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';
import { TransactionalOutboxInboxConfig } from './config';
import {
  TransactionalStrategies,
  createLogicalReplicationListener,
  __only_for_unit_tests__ as tests,
} from './logical-replication-listener';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000); // for the 5sec heartbeat
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
          new: dbMessage,
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

// A valid chunk that represents log message
const getReplicationChunk = (increment = 0) =>
  Buffer.from([
    119,
    0,
    0,
    0,
    0,
    93,
    162,
    168,
    increment,
    0,
    0,
    0,
    0,
    9,
    162,
    168,
    0,
    0,
    2,
    168,
    74,
    108,
    17,
    127,
    72,
    66,
    0,
    0,
    0,
    0,
    9,
    162,
    254,
    96,
    0,
    2,
    168,
    74,
    108,
    17,
    119,
    203,
    0,
    1,
    233,
    183,
  ]);
const sendReplicationChunk = (chunk: Buffer) => {
  (client as any).connection.emit('copyData', {
    chunk,
    length: chunk.length,
    name: 'copyData',
  });
};

const settings = {
  dbSchema: 'test_schema',
  dbTable: 'test_table',
  postgresPub: 'test_pub',
  postgresSlot: 'test_slot',
};

const dbMessage = {
  id: 'message_id',
  aggregate_type: 'test_aggregate_type',
  message_type: 'test_message_type',
  aggregate_id: 'test_aggregate_id',
  payload: { result: 'success' },
  metadata: { routingKey: 'test.route', exchange: 'test-exchange' },

  created_at: new Date('2023-01-18T21:02:27.000Z'),
};

const message = {
  id: dbMessage.id,
  aggregateType: dbMessage.aggregate_type,
  aggregateId: dbMessage.aggregate_id,
  messageType: dbMessage.message_type,
  payload: dbMessage.payload,
  metadata: dbMessage.metadata,
  createdAt: '2023-01-18T21:02:27.000Z',
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

const getStrategies = (
  config?: TransactionalOutboxInboxConfig,
): TransactionalStrategies => {
  return {
    concurrencyStrategy: defaultConcurrencyStrategy(),
    messageProcessingTimeoutStrategy: defaultMessageProcessingTimeoutStrategy(
      config ??
        ({
          settings: { ...settings, messageProcessingTimeout: 2_000 },
        } as TransactionalOutboxInboxConfig),
    ),
  };
};

describe('Local replication service unit tests', () => {
  describe('getRelevantMessage', () => {
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
          id: 'test_aggregate_id',
          aggregate_type: 'test_aggregate_type',
          aggregate_id: 'test_aggregate_id',
          message_type: 'test_message_type',
          payload: { test: 'payload' },
          metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
          created_at: new Date('2023-01-18T21:02:27.000Z'),
        },
      };

      // Act
      const message = tests?.getRelevantMessage?.(log, settings);

      // Assert
      expect(message).toEqual({
        id: 'test_aggregate_id',
        aggregateType: 'test_aggregate_type',
        aggregateId: 'test_aggregate_id',
        messageType: 'test_message_type',
        payload: { test: 'payload' },
        metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
        createdAt: '2023-01-18T21:02:27.000Z',
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
          aggregate_type: 'test_aggregate_type',
        }),
      ).toBeUndefined();
    });

    it('should return undefined if input has wrong types for required properties', () => {
      // Act + Assert
      expect(
        tests?.mapMessage?.({
          id: 123, // not a string
          aggregate_type: 'test_aggregate_type',
          aggregate_id: 'test_aggregate_id',
          message_type: 'test_message_type',
          created_at: 'not a date',
          payload: { test: 'payload' },
          metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
        }),
      ).toBeUndefined();
    });

    it('should return an OutboxMessage if input is valid', () => {
      // Arrange
      const input = {
        id: '123',
        aggregate_type: 'test_aggregate_type',
        aggregate_id: 'test_aggregate_id',
        message_type: 'test_message_type',
        created_at: new Date('2023-01-18T21:02:27.000Z'),
        payload: { test: 'payload' },
        metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      };

      // Act
      const message = tests?.mapMessage?.(input);

      // Assert
      expect(message).toBeDefined();
      expect(message).toEqual({
        id: input.id,
        aggregateType: input.aggregate_type,
        aggregateId: input.aggregate_id,
        messageType: input.message_type,
        payload: input.payload,
        metadata: input.metadata,
        createdAt: '2023-01-18T21:02:27.000Z',
      });
    });
  });

  describe('createService', () => {
    let messageHandler: jest.Mock;
    let errorHandler: jest.Mock;

    beforeEach(() => {
      messageHandler = jest.fn();
      errorHandler = jest.fn();

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

    it('should call the messageHandler and acknowledge the message when no errors are thrown', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const [cleanup] = createLogicalReplicationListener<OutboxMessage>(
        config,
        messageHandler,
        errorHandler,
        getDisabledLogger(),
        getStrategies(),
        'inbox',
      );
      await continueEventLoop();

      // Act
      sendReplicationChunk(getReplicationChunk(0));
      await continueEventLoop();

      // Assert
      expect(messageHandler).toHaveBeenCalledWith(message);
      expect(errorHandler).not.toHaveBeenCalled();
      expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
      expect(client.connect).toHaveBeenCalledTimes(1);
      await cleanup();
      expect(client.end).toHaveBeenCalledTimes(1);
    });

    it('should call the messageHandler but not acknowledge the message when a transient error is thrown', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const testError = new Error('Transient error');
      let errorHandlerCalled = false;
      const [cleanup] = createLogicalReplicationListener<OutboxMessage>(
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
        getDisabledLogger(),
        getStrategies(),
        'inbox',
      );
      await continueEventLoop();

      // Act
      sendReplicationChunk(getReplicationChunk(0));
      await continueEventLoop();

      // Assert
      expect(errorHandlerCalled).toBe(true);
      expect(errorHandler).not.toHaveBeenCalled();
      expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalled();
      expect(client.connect).toHaveBeenCalledTimes(1);
      await cleanup();
      expect(client.end).toHaveBeenCalledTimes(2); // once as part of error handling - once via shutdown
    });

    it('should call the messageHandler and acknowledge the message when a permanent error is thrown', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const testError = new Error('Unit test error');
      let errorHandlerCalled = false;
      const [cleanup] = createLogicalReplicationListener<OutboxMessage>(
        config,
        async () => {
          throw testError;
        },
        async (msg, error) => {
          expect(msg).toStrictEqual(message);
          expect(error).toStrictEqual(testError);
          errorHandlerCalled = true;
          return false;
        },
        getDisabledLogger(),
        getStrategies(config),
        'inbox',
      );
      await continueEventLoop();

      // Act
      sendReplicationChunk(getReplicationChunk(0));
      await continueEventLoop();

      // Assert
      expect(errorHandlerCalled).toBe(true);
      expect(errorHandler).not.toHaveBeenCalled();
      expect(client.connection.sendCopyFromChunk).toHaveBeenCalledTimes(1);
      expect(client.connect).toHaveBeenCalledTimes(1);
      await cleanup();
      expect(client.end).toHaveBeenCalledTimes(1);
    });

    it('A keep alive to which the service should respond is acknowledged', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const [cleanup] = createLogicalReplicationListener<OutboxMessage>(
        config,
        messageHandler,
        errorHandler,
        getDisabledLogger(),
        getStrategies(config),
        'inbox',
      );
      await continueEventLoop();

      // Act
      const mandatoryKeepAliveChunk = Buffer.from([
        107, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
      ]);
      sendReplicationChunk(mandatoryKeepAliveChunk);
      await continueEventLoop();

      // Assert
      expect(messageHandler).not.toHaveBeenCalled();
      expect(errorHandler).not.toHaveBeenCalled();
      expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
      expect(client.connect).toHaveBeenCalledTimes(1);
      await cleanup();
      expect(client.end).toHaveBeenCalledTimes(1);
    });

    it('A keep alive to which the service is not required to respond is not acknowledged', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      const [cleanup] = createLogicalReplicationListener<OutboxMessage>(
        config,
        messageHandler,
        errorHandler,
        getDisabledLogger(),
        getStrategies(config),
        'inbox',
      );
      await continueEventLoop();

      // Act
      const optionalKeepAliveChunk = Buffer.from([
        107, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      ]);
      sendReplicationChunk(optionalKeepAliveChunk);
      await continueEventLoop();

      // Assert
      expect(messageHandler).not.toHaveBeenCalled();
      expect(errorHandler).not.toHaveBeenCalled();
      expect(client.connection.sendCopyFromChunk).not.toHaveBeenCalled();
      expect(client.connect).toHaveBeenCalledTimes(1);
      await cleanup();
      expect(client.end).toHaveBeenCalledTimes(1);
    });

    it('Parallel messages wait to be executed sequentially', async () => {
      // Arrange
      const sleepTime = 50;
      const config = {
        pgReplicationConfig: {},
        settings,
      };
      let count = 0;
      const delayedMessageHandler = async () => {
        count++;
        await sleep(sleepTime);
      };
      const [cleanup] = createLogicalReplicationListener<OutboxMessage>(
        config,
        delayedMessageHandler,
        errorHandler,
        getDisabledLogger(),
        getStrategies(config),
        'inbox',
      );
      await continueEventLoop();

      // Act
      for (let i = 0; i < 10; i++) {
        sendReplicationChunk(getReplicationChunk(i));
      }
      await continueEventLoop();

      // Assert
      expect(count).toBeLessThan(10);
      await sleep(9 * sleepTime);
      expect(count).toBeLessThan(10);
      await sleep(sleepTime + 100);
      expect(count).toBe(10);
      expect(errorHandler).not.toHaveBeenCalled();
      expect(client.connection.sendCopyFromChunk).toHaveBeenCalledTimes(10);
      expect(client.connect).toHaveBeenCalledTimes(1);
      await cleanup();
      expect(client.end).toHaveBeenCalledTimes(1);
    });

    it('should call the messageHandler and then the errorHandler when the configured timeout is exceeded', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings: { ...settings, messageProcessingTimeout: 100 },
      };
      let messageHandlerCalled = false;
      let errorHandlerCalled = false;
      const [cleanup] = createLogicalReplicationListener<OutboxMessage>(
        config,
        async () => {
          messageHandlerCalled = true;
          await sleep(200);
        },
        async (msg, error) => {
          expect(msg).toStrictEqual(message);
          expect(error.message).toMatch(
            new RegExp(`Could not process the message with ID ${message.id}.*`),
          );
          errorHandlerCalled = true;
          return true;
        },
        getDisabledLogger(),
        getStrategies(config),
        'inbox',
      );
      await continueEventLoop();

      // Act
      sendReplicationChunk(getReplicationChunk(0));
      await sleep(250);

      // Assert
      expect(messageHandlerCalled).toBe(true);
      expect(errorHandlerCalled).toBe(true);
      expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
      await cleanup();
    });

    it('should call the messageHandler and then the errorHandler when the timeout strategy value is exceeded', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings: { ...settings, messageProcessingTimeout: 2_000 },
      };
      let messageHandlerCalled = false;
      let errorHandlerCalled = false;
      const [cleanup] = createLogicalReplicationListener<OutboxMessage>(
        config,
        async () => {
          messageHandlerCalled = true;
          await sleep(200);
        },
        async (msg, error) => {
          expect(msg).toStrictEqual(message);
          expect(error.message).toMatch(
            new RegExp(`Could not process the message with ID ${message.id}.*`),
          );
          errorHandlerCalled = true;
          return true;
        },
        getDisabledLogger(),
        {
          concurrencyStrategy: defaultConcurrencyStrategy(),
          messageProcessingTimeoutStrategy: () => 100,
        },
        'inbox',
      );
      await continueEventLoop();

      // Act
      sendReplicationChunk(getReplicationChunk(0));
      await sleep(250);

      // Assert
      expect(messageHandlerCalled).toBe(true);
      expect(errorHandlerCalled).toBe(true);
      expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
      await cleanup();
    });

    it('should call the messageHandler and acknowledge the message when the message does not run into a timeout', async () => {
      // Arrange
      const config = {
        pgReplicationConfig: {},
        settings: { ...settings, messageProcessingTimeout: 200 },
      };
      const [cleanup] = createLogicalReplicationListener<OutboxMessage>(
        config,
        messageHandler,
        errorHandler,
        getDisabledLogger(),
        {
          concurrencyStrategy: defaultConcurrencyStrategy(),
          messageProcessingTimeoutStrategy: () => 200,
        },
        'inbox',
      );
      await continueEventLoop();

      // Act
      sendReplicationChunk(getReplicationChunk(0));
      await continueEventLoop();

      // Assert
      expect(messageHandler).toHaveBeenCalledWith(message);
      expect(errorHandler).not.toHaveBeenCalled();
      expect(client.connection.sendCopyFromChunk).toHaveBeenCalled();
      expect(client.connect).toHaveBeenCalledTimes(1);
      await cleanup();
      expect(client.end).toHaveBeenCalledTimes(1);
    });
  });
});

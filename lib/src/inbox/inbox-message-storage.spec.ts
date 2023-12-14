/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Client, Pool, PoolClient } from 'pg';
import { getDisabledLogger, getInMemoryLogger } from '../common/logger';
import { InboxMessage, OutboxMessage } from '../common/message';
import { InboxConfig } from './inbox-listener';
import {
  PoisonousCheck,
  ackInbox,
  initializeInboxMessageStorage,
  nackInbox,
  poisonousMessageUpdate,
  verifyInbox,
} from './inbox-message-storage';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000);
}

const message: OutboxMessage = {
  id: 'message_id',
  aggregateType: 'test_type',
  messageType: 'test_message_type',
  aggregateId: 'test_aggregate_id',
  payload: { result: 'success' },
  metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
  createdAt: '2023-01-18T21:02:27.000Z',
};

const inboxMessage: InboxMessage = {
  ...message,
  startedAttempts: 1,
  finishedAttempts: 0,
  processedAt: null,
};

const config: InboxConfig = {
  pgConfig: {
    host: 'test_host',
    port: 5432,
    database: 'test_db',
    user: 'test_user',
    password: 'test_user_password',
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
    maxAttempts: 7,
  },
};

// required for the initializeInboxMessageStorage
let onPoolError: (err: Error) => void;
jest.mock('pg', () => {
  return {
    ...jest.requireActual('pg'),
    Pool: jest.fn().mockImplementation(() => ({
      connect: jest.fn(() => new Client()),
      on: jest.fn((_name, func) => {
        onPoolError = func;
      }),
      end: jest.fn(() => Promise.resolve()),
      removeAllListeners: jest.fn(),
    })),
    Client: jest.fn().mockImplementation(() => ({
      query: jest.fn(async (sql: string, params: [unknown]) => {
        if (params[0] === 'throw-error') {
          throw new Error('message not stored');
        }
        if (
          sql.indexOf(
            `INSERT INTO ${config.settings.dbSchema}.${config.settings.dbTable}`,
          ) &&
          params[0] === 'already-existed'
        ) {
          return { rowCount: 0 };
        }
        if (
          sql.indexOf(
            `INSERT INTO ${config.settings.dbSchema}.${config.settings.dbTable}`,
          )
        ) {
          return { rowCount: 1 };
        }
        throw new Error(`Missed to mock the following SQL query: ${sql}`);
      }),
      on: jest.fn(),
      release: jest.fn(),
    })),
  };
});

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

describe('Inbox unit tests', () => {
  describe('initializeInboxMessageStorage', () => {
    test('it initializes the inbox message storage and stores a message without an error', async () => {
      // Act
      const [storeInboxMessage, shutdown] = initializeInboxMessageStorage(
        config,
        getDisabledLogger(),
      );

      // Assert
      await expect(storeInboxMessage(message)).resolves.not.toThrow();
      await shutdown();
    });

    test('it logs an PostgreSQL error when it is raised', async () => {
      // Arrange
      const [logger, logs] = getInMemoryLogger('test');
      const error = new Error('Test');

      // Act
      const [_, shutdown] = initializeInboxMessageStorage(config, logger);
      onPoolError(error);

      // Assert
      expect(logs).toHaveLength(1);
      expect(logs[0].args).toEqual([error, 'PostgreSQL pool error']);
      await shutdown();
    });

    test('it initializes the inbox message storage and catches an error and logs it', async () => {
      // Act
      const [storeInboxMessage, shutdown] = initializeInboxMessageStorage(
        config,
        getDisabledLogger(),
      );
      // Let the `await pool.end();` throw an error
      (Pool as any).mock.results[0].value.end = () => {
        throw new Error('test');
      };

      // Assert
      await expect(
        storeInboxMessage({
          ...message,
          id: 'throw-error',
        }),
      ).rejects.toThrow(
        `Could not store the inbox message with id throw-error`,
      );
      await shutdown();
    });

    test('it catches an error during shutdown and logs it', async () => {
      // Arrange
      const [logger, logs] = getInMemoryLogger('test');
      const error = new Error('test');

      // Act
      const [_, shutdown] = initializeInboxMessageStorage(config, logger);

      // Assert
      // Let the `await pool.end();` throw an error
      const testPoolIndex = (Pool as any).mock.results.length;
      (Pool as any).mock.results[testPoolIndex - 1].value.end = () => {
        throw error;
      };
      await shutdown();
      expect(logs).toHaveLength(1);
      expect(logs[0].args).toEqual([
        error,
        'Inbox message storage shutdown error',
      ]);
      await shutdown();
    });

    test('it logs a warning when the message already existed', async () => {
      // Arrange
      const [logger, logs] = getInMemoryLogger('unit test');
      const [storeInboxMessage, shutdown] = initializeInboxMessageStorage(
        config,
        logger,
      );

      // Assert
      await expect(
        storeInboxMessage({ ...message, id: 'already-existed' }),
      ).resolves.not.toThrow();
      const log = logs.filter(
        (log) =>
          log.args[1] === 'The message with id already-existed already existed',
      );
      expect(log).toBeDefined();
      await shutdown();
    });
  });

  describe('poisonousMessageUpdate', () => {
    it('should update the started attempts and return the poisonous check', async () => {
      // Arrange
      const id = '1';
      const startedAttempts = 1;
      const finishedAttempts = 0;
      const inboxResult = {
        rowCount: 1,
        rows: [
          {
            started_attempts: startedAttempts + 1,
            finished_attempts: finishedAttempts,
          },
        ],
      };
      const pool = new Pool();
      const client = await pool.connect();
      client.query = jest.fn().mockResolvedValue(inboxResult);
      const settings = {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
      };
      const expected: PoisonousCheck = {
        startedAttempts: startedAttempts + 1,
        finishedAttempts: finishedAttempts,
      };

      // Act
      const result = await poisonousMessageUpdate(
        { id } as InboxMessage,
        client,
        { settings } as InboxConfig,
      );

      // Assert
      expect(client.query).toHaveBeenCalledTimes(1);
      expect(client.query).toHaveBeenCalledWith(
        expect.stringContaining(
          `UPDATE ${settings.dbSchema}.${settings.dbTable}`,
        ),
        [id],
      );
      expect(result).toEqual(expected);
    });

    it('should return undefined if the inbox message does not exist', async () => {
      // Arrange
      const id = '1';
      const inboxResult = {
        rowCount: 0,
        rows: [],
      };
      const pool = new Pool();
      const client = await pool.connect();
      client.query = jest.fn().mockResolvedValue(inboxResult);
      const settings = {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
      };

      // Act
      const result = await poisonousMessageUpdate(
        { id } as InboxMessage,
        client,
        { settings } as InboxConfig,
      );

      // Assert
      expect(client.query).toHaveBeenCalledTimes(1);
      expect(client.query).toHaveBeenCalledWith(
        expect.stringContaining(
          `UPDATE ${settings.dbSchema}.${settings.dbTable}`,
        ),
        [id],
      );
      expect(result).toBeUndefined();
    });
  });

  describe('verifyInbox', () => {
    test('it verifies the inbox message', async () => {
      // Arrange
      const pool = new Pool();
      const client = await pool.connect();

      // Act + Assert
      // Test for INBOX_MESSAGE_NOT_FOUND (no row found)
      client.query = jest.fn().mockResolvedValue({
        rowCount: 0,
      });
      let result = await verifyInbox(inboxMessage, client, config);
      expect(result).toBe('INBOX_MESSAGE_NOT_FOUND');

      // Test for ALREADY_PROCESSED (one row found but has a processed date)
      client.query = jest.fn().mockResolvedValue({
        rowCount: 1,
        rows: [{ processed_at: new Date() }],
      });
      result = await verifyInbox(inboxMessage, client, config);
      expect(result).toBe('ALREADY_PROCESSED');

      // Test for success (one row found that was not processed yet)
      client.query = jest.fn().mockResolvedValue({
        rowCount: 1,
        rows: [{ processed_at: null, finished_attempts: 0 }],
      });
      result = await verifyInbox(inboxMessage, client, config);
      expect(result).toBe(true);
    });

    it.each([0, 1, 2, 3, 4, 5, 6, 7, 100])(
      'should return "true" when the message is found and was not processed and add the values to the message: %p',
      async (finished_attempts) => {
        // Arrange
        const pool = new Pool();
        const client = await pool.connect();
        client.query = jest.fn().mockResolvedValue({
          rowCount: 1,
          rows: [
            {
              finished_attempts,
              started_attempts: finished_attempts + 1,
              processed_at: null,
            },
          ],
        });

        // Act
        const result = await verifyInbox(inboxMessage, client, config);

        // Assert
        expect(result).toBe(true);
        expect(inboxMessage.finishedAttempts).toBe(finished_attempts);
        expect(inboxMessage.startedAttempts).toBe(finished_attempts + 1);
        expect(inboxMessage.processedAt).toBeNull();
      },
    );
  });

  describe('ackInbox', () => {
    it('should call query with the correct parameters', async () => {
      // Arrange
      const pool = new Pool();
      const client = await pool.connect();
      client.query = jest.fn().mockResolvedValue({
        rowCount: 0,
      });

      // Act
      await ackInbox(inboxMessage, client, config);

      // Assert
      expect(client.query).toHaveBeenCalledWith(
        `UPDATE ${config.settings.dbSchema}.${config.settings.dbTable} SET processed_at = $1, finished_attempts = finished_attempts + 1 WHERE id = $2`,
        [expect.any(String), inboxMessage.id],
      );
    });
  });

  describe('nackInbox', () => {
    it('The nack logic increments the finished_attempts by one', async () => {
      // Arrange
      const pool = new Pool();
      const client = await pool.connect();
      client.query = jest.fn().mockResolvedValue({
        rowCount: 0,
        rows: [],
      });

      // Act
      await nackInbox({ ...inboxMessage }, client, config);

      // Assert
      expect(client.query).toHaveBeenCalledWith(
        expect.stringContaining(
          'UPDATE test_schema.test_table SET finished_attempts = finished_attempts + 1 WHERE id = $1',
        ),
        [inboxMessage.id],
      );
    });
  });
});

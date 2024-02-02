/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Pool, PoolClient } from 'pg';
import { getDisabledLogger, getInMemoryLogger } from '../common/logger';
import { ReplicationConfig } from '../replication/config';
import { StoredTransactionalMessage, TransactionalMessage } from './message';
import {
  increaseMessageFinishedAttempts,
  initializeMessageStorage,
  initiateMessageProcessing,
  markMessageCompleted,
  startedAttemptsIncrement,
} from './message-storage';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000);
}

const message: TransactionalMessage = {
  id: 'message_id',
  aggregateType: 'test_type',
  messageType: 'test_message_type',
  aggregateId: 'test_aggregate_id',
  payload: { result: 'success' },
  metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
  createdAt: '2023-01-18T21:02:27.000Z',
};

const storedMessage: StoredTransactionalMessage = {
  ...message,
  startedAttempts: 1,
  finishedAttempts: 0,
  processedAt: null,
};

const config: ReplicationConfig = {
  outboxOrInbox: 'inbox',
  dbHandlerConfig: {
    host: 'test_host',
    port: 5432,
    database: 'test_db',
    user: 'test_user',
    password: 'test_user_password',
  },
  dbListenerConfig: {
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

describe('Message storage unit tests', () => {
  let pool: Pool;
  let client: PoolClient;
  beforeEach(() => {
    client = {
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
    } as unknown as PoolClient;
    pool = {
      connect: () => client,
      on: jest.fn(),
      release: jest.fn(),
    } as unknown as Pool;
  });

  describe('initializeMessageStorage', () => {
    test('it initializes the message storage and stores a message without an error', async () => {
      // Act
      const storeMessage = initializeMessageStorage(
        config,
        getDisabledLogger(),
      );

      // Assert
      await expect(
        storeMessage(message, await pool.connect()),
      ).resolves.not.toThrow();
    });

    test('it initializes the message storage and catches an error and logs it', async () => {
      // Act
      const storeMessage = initializeMessageStorage(
        config,
        getDisabledLogger(),
      );
      client.query = () => {
        throw new Error();
      };

      // Assert
      await expect(
        storeMessage(
          {
            ...message,
            id: 'throw-error',
          },
          await pool.connect(),
        ),
      ).rejects.toThrow(
        `Could not store the inbox message with id throw-error`,
      );
    });

    test('it logs a warning when the message already existed', async () => {
      // Arrange
      const [logger, logs] = getInMemoryLogger('unit test');
      const storeMessage = initializeMessageStorage(config, logger);

      // Assert
      await expect(
        storeMessage(
          { ...message, id: 'already-existed' },
          await pool.connect(),
        ),
      ).resolves.not.toThrow();
      const log = logs.filter(
        (log) =>
          log.args[1] === 'The message with id already-existed already existed',
      );
      expect(log).toHaveLength(1);
    });
  });

  describe('startedAttemptsIncrement', () => {
    let message: StoredTransactionalMessage;
    const client = {
      query: jest.fn().mockResolvedValue({
        rowCount: 1,
        rows: [
          { started_attempts: 1, finished_attempts: 0, processed_at: null },
        ],
      }),
    } as unknown as PoolClient;

    beforeEach(() => {
      // Initialize your mock message, client, and config here
      message = {
        id: '005c0b04-f8e0-4170-abce-8ee8dbbfe790',
        aggregateType: 'test-aggregate',
        aggregateId: 'test_aggregate_id',
        messageType: 'test-message',
        payload: { result: 'success' },
        metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
        createdAt: '2023-01-18T21:02:27.000Z',
        // Not yet filled: startedAttempts, finishedAttempts, and processedAt
      } as unknown as StoredTransactionalMessage;
    });

    it('should increment started_attempts and return true for unprocessed messages', async () => {
      // Arrange
      client.query = jest.fn().mockResolvedValue({
        rowCount: 1,
        rows: [
          { started_attempts: 1, finished_attempts: 0, processed_at: null },
        ],
      });

      // Act
      const result = await startedAttemptsIncrement(message, client, config);

      // Assert
      expect(result).toBe(true);
      expect(message.startedAttempts).toBe(1);
      expect(message.finishedAttempts).toBe(0);
      expect(message.processedAt).toBeNull();
    });

    it.each([0, 1, 2, 3, 4, 5, 6, 7, 100])(
      'should return "true" when the message is found and was not processed and add the values to the message: %p',
      async (finished_attempts) => {
        // Arrange
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
        const result = await startedAttemptsIncrement(
          storedMessage,
          client,
          config,
        );

        // Assert
        expect(result).toBe(true);
        expect(storedMessage.finishedAttempts).toBe(finished_attempts);
        expect(storedMessage.startedAttempts).toBe(finished_attempts + 1);
        expect(storedMessage.processedAt).toBeNull();
      },
    );

    it('should return "MESSAGE_NOT_FOUND" for non-existent messages', async () => {
      // Arrange
      client.query = jest.fn().mockResolvedValue({ rowCount: 0 });

      // Act
      const result = await startedAttemptsIncrement(message, client, config);

      // Assert
      expect(result).toBe('MESSAGE_NOT_FOUND');
    });

    it('should return "ALREADY_PROCESSED" for already processed messages', async () => {
      // Arrange
      client.query = jest.fn().mockResolvedValue({
        rowCount: 1,
        rows: [
          {
            started_attempts: 1,
            finished_attempts: 1,
            processed_at: new Date().toISOString(),
          },
        ],
      });

      // Act
      const result = await startedAttemptsIncrement(message, client, config);

      // Assert
      expect(result).toBe('ALREADY_PROCESSED');
    });
  });

  describe('initiateMessageProcessing', () => {
    test('it verifies the message', async () => {
      // Act + Assert
      // Test for MESSAGE_NOT_FOUND (no row found)
      client.query = jest.fn().mockResolvedValue({
        rowCount: 0,
      });
      let result = await initiateMessageProcessing(
        { ...storedMessage },
        client,
        config,
      );
      expect(result).toBe('MESSAGE_NOT_FOUND');

      // Test for ALREADY_PROCESSED (one row found but has a processed date)
      client.query = jest.fn().mockResolvedValue({
        rowCount: 1,
        rows: [{ processed_at: new Date() }],
      });
      result = await initiateMessageProcessing(
        { ...storedMessage },
        client,
        config,
      );
      expect(result).toBe('ALREADY_PROCESSED');

      // Test for success (one row found that was not processed yet)
      client.query = jest.fn().mockResolvedValue({
        rowCount: 1,
        rows: [{ processed_at: null, finished_attempts: 0 }],
      });
      result = await initiateMessageProcessing(
        { ...storedMessage },
        client,
        config,
      );
      expect(result).toBe(true);
    });

    test('it verifies that it updates the message properties when it was processed', async () => {
      // Arrange
      client.query = jest.fn().mockResolvedValue({
        rowCount: 1,
        rows: [
          {
            started_attempts: 4,
            finished_attempts: 3,
            processed_at: '2023-01-18T21:02:27.000Z',
          },
        ],
      });
      const msg = { ...storedMessage };

      // Act
      const result = await initiateMessageProcessing(msg, client, config);

      // Assert
      expect(result).toBe('ALREADY_PROCESSED');
      expect(msg).toMatchObject<
        Pick<
          StoredTransactionalMessage,
          'startedAttempts' | 'finishedAttempts' | 'processedAt'
        >
      >({
        startedAttempts: 4,
        finishedAttempts: 3,
        processedAt: '2023-01-18T21:02:27.000Z',
      });
    });

    test('it verifies that it updates the message properties when it was not processed', async () => {
      // Arrange
      client.query = jest.fn().mockResolvedValue({
        rowCount: 1,
        rows: [
          {
            started_attempts: 4,
            finished_attempts: 3,
            processed_at: null,
          },
        ],
      });
      const msg = { ...storedMessage };

      // Act
      const result = await initiateMessageProcessing(msg, client, config);

      // Assert
      expect(result).toBe(true);
      expect(msg).toMatchObject<
        Pick<
          StoredTransactionalMessage,
          'startedAttempts' | 'finishedAttempts' | 'processedAt'
        >
      >({
        startedAttempts: 4,
        finishedAttempts: 3,
        processedAt: null,
      });
    });
  });

  describe('markMessageCompleted', () => {
    it('should call query with the correct parameters', async () => {
      // Arrange
      client.query = jest.fn().mockResolvedValue({
        rowCount: 0,
      });

      // Act
      await markMessageCompleted(storedMessage, client, config);

      // Assert
      expect(client.query).toHaveBeenCalledWith(
        `UPDATE ${config.settings.dbSchema}.${config.settings.dbTable} SET processed_at = $1, finished_attempts = finished_attempts + 1 WHERE id = $2`,
        [expect.any(String), storedMessage.id],
      );
    });
  });

  describe('nackMessage', () => {
    it('The nack logic increments the finished_attempts by one', async () => {
      // Arrange
      client.query = jest.fn().mockResolvedValue({
        rowCount: 0,
        rows: [],
      });

      // Act
      await increaseMessageFinishedAttempts(
        { ...storedMessage },
        client,
        config,
      );

      // Assert
      expect(client.query).toHaveBeenCalledWith(
        expect.stringContaining(
          'UPDATE test_schema.test_table SET finished_attempts = finished_attempts + 1 WHERE id = $1',
        ),
        [storedMessage.id],
      );
    });
  });
});

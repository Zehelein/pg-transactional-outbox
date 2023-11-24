/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Client, Pool } from 'pg';
import { OutboxConfig } from './outbox-listener';
import { initializeOutboxMessageStorage } from './outbox-message-storage';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000);
}

const config: OutboxConfig = {
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

// required for the initializeOutboxMessageStorage
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
      query: jest.fn(async (sql: string, _params: [unknown]) => {
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

describe('Outbox unit tests', () => {
  describe('initializeOutboxMessageStorage', () => {
    it('should store the outbox message data to the database', async () => {
      // Arrange
      const pool = new Pool();
      const client = await pool.connect();
      client.query = jest
        .fn()
        .mockReturnValueOnce({
          rowCount: 1,
          rows: [{ created_at: new Date() }],
        })
        .mockReturnValueOnce({});
      const aggregateId = 'test-aggregate-id';
      const aggregateType = 'movie';
      const messageType = 'created';
      const payload = { test: 'data' };
      const metadata = { routingKey: 'test.route', exchange: 'test-exchange' };
      const storeOutboxMessage = initializeOutboxMessageStorage(
        aggregateType,
        messageType,
        config,
      );

      // Act
      const result = await storeOutboxMessage(
        aggregateId,
        payload,
        client,
        metadata,
      );

      // Assert
      expect(client.query).toHaveBeenCalledTimes(2);
      expect(client.query).toHaveBeenCalledWith(
        expect.stringContaining(
          `INSERT INTO ${config.settings.dbSchema}.${config.settings.dbTable}`,
        ),
        [
          expect.any(String),
          aggregateType,
          aggregateId,
          messageType,
          payload,
          metadata,
        ],
      );
      expect(result).toEqual({
        id: expect.any(String),
        aggregateType,
        aggregateId,
        messageType,
        payload,
        metadata,
        createdAt: expect.any(Date),
      });
    });

    it('should throw an error if the outbox message could not be created', async () => {
      // Arrange
      const pool = new Pool();
      const client = await pool.connect();
      client.query = jest.fn().mockReturnValue({ rowCount: 0 });
      const storeOutboxMessage = initializeOutboxMessageStorage(
        'movie',
        'created',
        config,
      );

      // Act + Assert
      await expect(
        storeOutboxMessage('test-aggregate-id', { test: 'data' }, client),
      ).rejects.toThrow('Could not insert the message into the outbox!');
    });
  });
});

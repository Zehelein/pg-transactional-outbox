/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Pool } from 'pg';
import { DatabaseClient } from '../common/database';
import { ListenerConfig } from '../common/listener-config';
import { getDisabledLogger, getInMemoryLogger } from '../common/logger';
import { initializeMessageStorage } from './initialize-message-storage';
import { TransactionalMessage } from './transactional-message';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000);
}

const message: TransactionalMessage = {
  id: 'message_id',
  aggregateType: 'test_aggregate_type',
  messageType: 'test_message_type',
  aggregateId: 'test_aggregate_id',
  payload: { result: 'success' },
  segment: 'test_segment',
  metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
};

const config: ListenerConfig = {
  outboxOrInbox: 'inbox',
  dbHandlerConfig: {
    host: 'test_host',
    port: 5432,
    database: 'test_db',
    user: 'test_inbox_handler',
    password: 'test_inbox_handler_password',
  },
  dbListenerConfig: {
    host: 'test_host',
    port: 5432,
    database: 'test_db',
    user: 'test_inbox_listener',
    password: 'test_inbox_listener_password',
  },
  settings: {
    dbSchema: 'test_schema',
    dbTable: 'test_table',
    maxAttempts: 7,
  },
};

describe('initializeMessageStorage', () => {
  let pool: Pool;
  let client: DatabaseClient;
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
    } as unknown as DatabaseClient;
    pool = {
      connect: () => client,
      on: jest.fn(),
      release: jest.fn(),
    } as unknown as Pool;
  });

  test('it stores the message with all provided values without an error', async () => {
    // Act
    const storeMessage = initializeMessageStorage(config, getDisabledLogger());
    const msg: TransactionalMessage = {
      ...message,
      segment: 'provided_segment',
      concurrency: 'parallel',
      createdAt: '2023-01-18T21:02:27.000Z',
      lockedUntil: '2023-01-18T21:04:27.000Z',
    };

    // Act + Assert
    await expect(
      storeMessage(msg, await pool.connect()),
    ).resolves.not.toThrow();
    expect(client.query).toHaveBeenCalledWith(
      `
    INSERT INTO test_schema.test_table
      (id, aggregate_type, aggregate_id, message_type, segment, payload, metadata, concurrency, created_at, locked_until)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT (id) DO NOTHING`,
      [
        'message_id',
        'test_aggregate_type',
        'test_aggregate_id',
        'test_message_type',
        'provided_segment',
        { result: 'success' },
        { routingKey: 'test.route', exchange: 'test-exchange' },
        'parallel',
        '2023-01-18T21:02:27.000Z',
        '2023-01-18T21:04:27.000Z',
      ],
    );
  });

  test('it stores the message with some missing values without an error', async () => {
    // Act
    const storeMessage = initializeMessageStorage(config, getDisabledLogger());
    const msg: TransactionalMessage = {
      ...message,
      segment: 'provided_segment',
      lockedUntil: '2023-01-18T21:04:27.000Z',
    };

    // Act + Assert
    await expect(
      storeMessage(msg, await pool.connect()),
    ).resolves.not.toThrow();
    expect(client.query).toHaveBeenCalledWith(
      `
    INSERT INTO test_schema.test_table
      (id, aggregate_type, aggregate_id, message_type, segment, payload, metadata, locked_until)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (id) DO NOTHING`,
      [
        'message_id',
        'test_aggregate_type',
        'test_aggregate_id',
        'test_message_type',
        'provided_segment',
        { result: 'success' },
        { routingKey: 'test.route', exchange: 'test-exchange' },
        '2023-01-18T21:04:27.000Z',
      ],
    );
  });

  test('it stores a message without an error when optional values are skipped', async () => {
    // Arrange
    client.query = jest.fn(() => ({ rowCount: 1 })) as any;
    const storeMessage = initializeMessageStorage(config, getDisabledLogger());

    // Act + Assert
    await expect(
      storeMessage(message, await pool.connect()),
    ).resolves.not.toThrow();
    expect(client.query).toHaveBeenCalledWith(
      `
    INSERT INTO test_schema.test_table
      (id, aggregate_type, aggregate_id, message_type, segment, payload, metadata)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (id) DO NOTHING`,
      [
        'message_id',
        'test_aggregate_type',
        'test_aggregate_id',
        'test_message_type',
        'test_segment',
        { result: 'success' },
        { routingKey: 'test.route', exchange: 'test-exchange' },
      ],
    );
  });

  test('it initializes the message storage and catches an error and logs it', async () => {
    // Act
    const storeMessage = initializeMessageStorage(config, getDisabledLogger());
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
    ).rejects.toThrow(`Could not store the inbox message with id throw-error`);
  });

  test('it logs a warning when the message already existed', async () => {
    // Arrange
    const [logger, logs] = getInMemoryLogger('unit test');
    const storeMessage = initializeMessageStorage(config, logger);

    // Assert
    await expect(
      storeMessage({ ...message, id: 'already-existed' }, await pool.connect()),
    ).resolves.not.toThrow();
    const log = logs.filter(
      (log) =>
        log.args[1] === 'The message with id already-existed already existed',
    );
    expect(log).toHaveLength(1);
  });
});

/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { Pool, PoolClient } from 'pg';
import { ListenerConfig } from '../common/base-config';
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
  aggregateType: 'test_type',
  messageType: 'test_message_type',
  aggregateId: 'test_aggregate_id',
  payload: { result: 'success' },
  metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
  createdAt: '2023-01-18T21:02:27.000Z',
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

  test('it initializes the message storage and stores a message without an error', async () => {
    // Act
    const storeMessage = initializeMessageStorage(config, getDisabledLogger());

    // Assert
    await expect(
      storeMessage(message, await pool.connect()),
    ).resolves.not.toThrow();
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

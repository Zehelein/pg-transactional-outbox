import { Pool, PoolClient } from 'pg';
import { getInMemoryLogger } from '../../dist';
import { ListenerConfig } from '../common/base-config';
import { sleep } from '../common/utils';
import {
  runMessageCleanupOnce,
  runScheduledMessageCleanup,
} from './message-cleanup';

jest.mock('pg', () => ({
  PoolClient: jest.fn(),
}));

describe('deleteMessagesCompleted', () => {
  let cleanupTimeout: NodeJS.Timeout | undefined;
  afterEach(() => clearInterval(cleanupTimeout));

  it('generates the correct SQL query when all three values are provided', async () => {
    // Arrange
    const settings = {
      dbSchema: 'inbox',
      dbTable: 'inbox',
      messageCleanupProcessed: 5000,
      messageCleanupAbandoned: 10000,
      messageCleanupAll: 20000,
    };
    const client = {
      query: jest.fn().mockReturnValue({ rowCount: 12 }),
    } as unknown as PoolClient;

    // Act
    const deleted = await runMessageCleanupOnce(client, { settings });

    // Assert
    expect(client.query).toHaveBeenCalledWith(
      "DELETE FROM inbox.inbox WHERE false OR processed_at < NOW() - ($1 || ' SECOND')::INTERVAL OR abandoned_at < NOW() - ($2 || ' SECOND')::INTERVAL OR created_at < NOW() - ($3 || ' SECOND')::INTERVAL RETURNING id;",
      [
        settings.messageCleanupProcessed,
        settings.messageCleanupAbandoned,
        settings.messageCleanupAll,
      ],
    );
    expect(deleted).toBe(12);
  });

  it('generates the correct SQL query when one value is missing', async () => {
    // Arrange
    const settings = {
      dbSchema: 'inbox',
      dbTable: 'inbox',
      messageCleanupAbandoned: 10000,
      messageCleanupAll: 20000,
    };
    const client = {
      query: jest.fn().mockReturnValue({ rowCount: 0 }),
    } as unknown as PoolClient;

    // Act
    await runMessageCleanupOnce(client, { settings });

    // Assert
    expect(client.query).toHaveBeenCalledWith(
      "DELETE FROM inbox.inbox WHERE false OR abandoned_at < NOW() - ($1 || ' SECOND')::INTERVAL OR created_at < NOW() - ($2 || ' SECOND')::INTERVAL RETURNING id;",
      [settings.messageCleanupAbandoned, settings.messageCleanupAll],
    );
  });

  it('does not call the query function when no properties were provided', async () => {
    // Mock input parameters (seconds)
    const settings = {
      dbSchema: 'inbox',
      dbTable: 'inbox',
    };
    const client = {
      query: jest.fn(),
    } as unknown as PoolClient;

    // Act
    await runMessageCleanupOnce(client, { settings });

    // Assert
    expect(client.query).not.toHaveBeenCalled();
  });

  it('runs the scheduler correctly', async () => {
    const settings = {
      dbSchema: 'inbox',
      dbTable: 'inbox',
      messageCleanupInterval: 1,
      messageCleanupAll: 20000,
    };
    const client = {
      query: jest.fn().mockReturnValue({ rowCount: 12 }),
    } as unknown as Pool;
    const [logger, logs] = getInMemoryLogger('test');

    const expectQueryCalled = () =>
      expect(client.query).toHaveBeenCalledWith(
        "DELETE FROM inbox.inbox WHERE false OR created_at < NOW() - ($1 || ' SECOND')::INTERVAL RETURNING id;",
        [settings.messageCleanupAll],
      );

    // Act and Assert
    const timeout = runScheduledMessageCleanup(
      client,
      { settings } as ListenerConfig,
      logger,
    );
    cleanupTimeout = timeout;
    expect(timeout).toBeDefined();

    await sleep(1100);
    expectQueryCalled();

    await sleep(1000);
    clearInterval(timeout);
    expectQueryCalled();
    client.query = jest.fn().mockReturnValue({ rowCount: 12 });
    await sleep(1000);
    expect(client.query).not.toHaveBeenCalled();
    expect(logs.filter((l) => l.type === 'error')).toHaveLength(0);
  });

  it('does not run the scheduler if no interval is set', async () => {
    const settings = {
      dbSchema: 'inbox',
      dbTable: 'inbox',
      messageCleanupInterval: undefined,
      messageCleanupAll: 20000,
    };
    const client = {
      query: jest.fn().mockReturnValue({ rowCount: 6 }),
    } as unknown as Pool;
    const [logger, logs] = getInMemoryLogger('test');

    // Act and Assert
    const timeout = runScheduledMessageCleanup(
      client,
      { settings } as ListenerConfig,
      logger,
    );
    cleanupTimeout = timeout;
    expect(timeout).toBeUndefined();

    await sleep(1100);
    expect(client.query).not.toHaveBeenCalled();
    expect(logs.filter((l) => l.type === 'error')).toHaveLength(0);
  });
});

/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { DatabaseClient } from '../common/database';
import { ListenerConfig } from '../common/listener-config';
import { startedAttemptsIncrement } from './started-attempts-increment';
import { StoredTransactionalMessage } from './transactional-message';

const config = {
  settings: {
    dbSchema: 'test_schema',
    dbTable: 'test_table',
  },
} as ListenerConfig;

describe('startedAttemptsIncrement', () => {
  let message: StoredTransactionalMessage;
  const client = {
    query: jest.fn(),
  } as unknown as DatabaseClient;

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
      // Not yet filled: startedAttempts, finishedAttempts, lockedUntil, processedAt, and abandonedAt
    } as unknown as StoredTransactionalMessage;
  });

  it('should increment started_attempts and return true for unprocessed messages', async () => {
    // Arrange
    client.query = jest.fn().mockResolvedValue({
      rowCount: 1,
      rows: [
        {
          started_attempts: 1,
          finished_attempts: 0,
          locked_until: null,
          processed_at: null,
          abandoned_at: null,
        },
      ],
    });

    // Act
    const result = await startedAttemptsIncrement(message, client, config);

    // Assert
    expect(result).toBe(true);
    expect(message.startedAttempts).toBe(1);
    expect(message.finishedAttempts).toBe(0);
    expect(message.lockedUntil).toBeNull();
    expect(message.processedAt).toBeNull();
    expect(message.abandonedAt).toBeNull();
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
            locked_until: null,
            processed_at: null,
            abandoned_at: null,
          },
        ],
      });

      // Act
      const result = await startedAttemptsIncrement(message, client, config);

      // Assert
      expect(result).toBe(true);
      expect(message.finishedAttempts).toBe(finished_attempts);
      expect(message.startedAttempts).toBe(finished_attempts + 1);
      expect(message.lockedUntil).toBeNull();
      expect(message.processedAt).toBeNull();
      expect(message.abandonedAt).toBeNull();
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
          locked_until: null,
          processed_at: new Date(),
          abandoned_at: null,
        },
      ],
    });

    // Act
    const result = await startedAttemptsIncrement(message, client, config);

    // Assert
    expect(result).toBe('ALREADY_PROCESSED');
  });

  it('should return "ABANDONED_MESSAGE" for already abandoned messages', async () => {
    // Arrange
    client.query = jest.fn().mockResolvedValue({
      rowCount: 1,
      rows: [
        {
          started_attempts: 1,
          finished_attempts: 1,
          locked_until: null,
          processed_at: null,
          abandoned_at: new Date(),
        },
      ],
    });

    // Act
    const result = await startedAttemptsIncrement(message, client, config);

    // Assert
    expect(result).toBe('ABANDONED_MESSAGE');
  });
});

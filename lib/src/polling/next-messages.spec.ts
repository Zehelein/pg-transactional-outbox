import { ClientBase } from 'pg';
import { BaseLogger } from 'pino';
import { getDisabledLogger } from '../common/logger';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { FullPollingListenerSettings } from './config';
import { getNextMessagesBatch } from './next-messages';

describe('getNextMessagesBatch', () => {
  it('should return an empty array when no messages are returned', async () => {
    // Arrange
    const mockClient: ClientBase = {
      query: jest.fn().mockResolvedValue({
        rows: [],
        rowCount: 0,
      }),
    } as unknown as ClientBase;
    const mockSettings = {
      settings: {
        nextMessagesFunctionSchema: 'test',
        nextMessagesFunctionName: 'test_load_next_messages',
      },
    } as unknown as FullPollingListenerSettings;
    const logger = {
      debug: jest.fn(),
      trace: jest.fn(),
    };

    // Act
    const result = await getNextMessagesBatch(
      10,
      mockClient,
      mockSettings,
      logger as unknown as BaseLogger,
      'outbox',
    );

    // Assert
    expect(result).toHaveLength(0);
    expect(logger.debug).not.toHaveBeenCalled();
    expect(logger.trace).toHaveBeenCalledWith(
      `Found no unprocessed outbox messages in the last minute.`,
    );
  });

  it('should fetch inbox messages with all provided details', async () => {
    // Arrange
    const mockClient: ClientBase = {
      query: jest.fn().mockResolvedValue({
        rows: [
          {
            id: '537d399e-291c-499c-a8cc-6bfa7e222b7c',
            aggregate_type: 'movie',
            message_type: 'movie_created',
            aggregate_id: 'test_aggregate_id',
            concurrency: 'sequential',
            payload: { result: 'success' },
            metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
            created_at: new Date('2023-01-18T21:02:27.000Z'),
            started_attempts: 2,
            finished_attempts: 2,
            locked_until: new Date('2023-01-18T21:01:27.000Z'),
            processed_at: new Date('2023-01-18T21:02:27.000Z'),
            abandoned_at: new Date('2023-01-18T21:03:27.000Z'),
          },
        ],
        rowCount: 1,
      }),
    } as unknown as ClientBase;
    const mockSettings = {
      settings: {
        nextMessagesFunctionSchema: 'test',
        nextMessagesFunctionName: 'test_load_next_messages',
      },
    } as unknown as FullPollingListenerSettings;
    const logger = {
      debug: jest.fn(),
      trace: jest.fn(),
    };

    // Act
    const result = await getNextMessagesBatch(
      10,
      mockClient,
      mockSettings,
      logger as unknown as BaseLogger,
      'inbox',
    );

    // Assert
    expect(result).toHaveLength(1);
    expect(result[0]).toStrictEqual<StoredTransactionalMessage>({
      id: '537d399e-291c-499c-a8cc-6bfa7e222b7c',
      aggregateType: 'movie',
      messageType: 'movie_created',
      aggregateId: 'test_aggregate_id',
      concurrency: 'sequential',
      segment: undefined,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      createdAt: '2023-01-18T21:02:27.000Z',
      startedAttempts: 2,
      finishedAttempts: 2,
      lockedUntil: '2023-01-18T21:01:27.000Z',
      processedAt: '2023-01-18T21:02:27.000Z',
      abandonedAt: '2023-01-18T21:03:27.000Z',
    });
    expect(logger.trace).not.toHaveBeenCalled();
    expect(logger.debug).toHaveBeenCalledWith(
      expect.any(Array),
      `Found 1 inbox message(s) to process.`,
    );
  });

  it('should fetch inbox messages and use fallbacks', async () => {
    // Arrange
    const mockClient: ClientBase = {
      query: jest.fn().mockResolvedValue({
        rows: [
          {
            id: '537d399e-291c-499c-a8cc-6bfa7e222b7c',
            aggregate_type: 'movie',
            message_type: 'movie_created',
            aggregate_id: 'test_aggregate_id',
            payload: { result: 'success' },
            metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
            started_attempts: 2,
            finished_attempts: 2,
            processed_at: null,
            abandoned_at: null,
            // Database defaults:
            concurrency: 'sequential',
            created_at: new Date('2023-01-18T21:02:27.000Z'),
            locked_until: new Date('1970-01-01T00:00:00.000Z'),
          },
        ],
        rowCount: 1,
      }),
    } as unknown as ClientBase;
    const mockSettings = {
      settings: {
        nextMessagesFunctionSchema: 'test',
        nextMessagesFunctionName: 'test_load_next_messages',
      },
    } as unknown as FullPollingListenerSettings;

    // Act
    const result = await getNextMessagesBatch(
      10,
      mockClient,
      mockSettings,
      getDisabledLogger(),
      'inbox',
    );

    // Assert
    expect(result).toHaveLength(1);
    expect(result[0]).toStrictEqual<StoredTransactionalMessage>({
      id: '537d399e-291c-499c-a8cc-6bfa7e222b7c',
      aggregateType: 'movie',
      messageType: 'movie_created',
      aggregateId: 'test_aggregate_id',
      segment: undefined,
      payload: { result: 'success' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      startedAttempts: 2,
      finishedAttempts: 2,
      processedAt: null,
      abandonedAt: null,
      // Database defaults:
      concurrency: 'sequential',
      createdAt: '2023-01-18T21:02:27.000Z',
      lockedUntil: '1970-01-01T00:00:00.000Z',
    });
  });

  it('should log trace messages only once a minute', async () => {
    // Arrange
    const mockClient: ClientBase = {
      query: jest
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              id: '537d399e-291c-499c-a8cc-6bfa7e222b7c',
              aggregate_type: 'movie',
              message_type: 'movie_created',
              aggregate_id: 'test_aggregate_id',
              concurrency: 'sequential',
              payload: { result: 'success' },
              metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
              created_at: new Date('2023-01-18T21:02:27.000Z'),
              started_attempts: 2,
              finished_attempts: 2,
              locked_until: new Date('2023-01-18T21:01:27.000Z'),
              processed_at: new Date('2023-01-18T21:02:27.000Z'),
              abandoned_at: new Date('2023-01-18T21:03:27.000Z'),
            },
          ],
          rowCount: 1,
        })
        .mockResolvedValue({
          rows: [],
          rowCount: 0,
        }),
    } as unknown as ClientBase;
    const mockSettings = {
      settings: {
        nextMessagesFunctionSchema: 'test',
        nextMessagesFunctionName: 'test_load_next_messages',
      },
    } as unknown as FullPollingListenerSettings;
    const logger = {
      debug: jest.fn(),
      trace: jest.fn(),
    };
    const next = async () =>
      getNextMessagesBatch(
        10,
        mockClient,
        mockSettings,
        logger as unknown as BaseLogger,
        'outbox',
      );

    // Act and Assert

    // Get one outbox message so the log is initialized
    jest.spyOn(Date, 'now').mockReturnValueOnce(1707987600000); // 2024-02-15T10:00:00
    let result = await next();
    expect(result).toHaveLength(1);
    expect(logger.debug).toHaveBeenCalled();
    expect(logger.trace).not.toHaveBeenCalled();
    logger.debug.mockReset();
    logger.trace.mockReset();

    jest.spyOn(Date, 'now').mockReturnValue(1707987630000); // 2024-02-15T10:00:30
    result = await next();
    expect(result).toHaveLength(0);
    expect(logger.debug).not.toHaveBeenCalled();
    expect(logger.trace).not.toHaveBeenCalled();
    logger.trace.mockReset();

    jest.spyOn(Date, 'now').mockReturnValue(1707987660000); // 2024-02-15T10:01:00
    result = await next();
    expect(result).toHaveLength(0);
    expect(logger.debug).not.toHaveBeenCalled();
    expect(logger.trace).toHaveBeenCalled();
    logger.trace.mockReset();

    jest.spyOn(Date, 'now').mockReturnValue(1707987690000); // 2024-02-15T10:01:30
    result = await next();
    expect(result).toHaveLength(0);
    expect(logger.debug).not.toHaveBeenCalled();
    expect(logger.trace).not.toHaveBeenCalled();
    logger.trace.mockReset();

    jest.spyOn(Date, 'now').mockReturnValue(1707987720000); // 2024-02-15T10:02:00
    result = await next();
    expect(result).toHaveLength(0);
    expect(logger.debug).not.toHaveBeenCalled();
    expect(logger.trace).toHaveBeenCalled();
    logger.trace.mockReset();
  });
});

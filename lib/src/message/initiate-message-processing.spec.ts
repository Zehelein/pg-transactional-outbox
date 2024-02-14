/* eslint-disable @typescript-eslint/no-non-null-assertion */
import inspector from 'inspector';
import { DatabaseClient } from '../common/database';
import { ReplicationListenerSettings } from '../replication/config';
import { initiateMessageProcessing } from './initiate-message-processing';
import {
  StoredTransactionalMessage,
  TransactionalMessage,
} from './transactional-message';

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
};

const storedMessage: StoredTransactionalMessage = {
  ...message,
  startedAttempts: 1,
  finishedAttempts: 0,
  concurrency: 'sequential',
  createdAt: '2023-01-18T21:02:27.000Z',
  lockedUntil: '2023-01-18T21:05:27.000Z',
  processedAt: null,
  abandonedAt: null,
};

const settings = {
  dbSchema: 'test_schema',
  dbTable: 'test_table',
  dbPublication: 'test_pub',
  dbReplicationSlot: 'test_slot',
} as ReplicationListenerSettings;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const getTestClient = (resolveValue: any) =>
  ({
    query: jest.fn().mockResolvedValue(resolveValue),
  }) as unknown as DatabaseClient;

describe('initiateMessageProcessing', () => {
  test('it verifies the message', async () => {
    // Act + Assert
    // Test for MESSAGE_NOT_FOUND (no row found)
    const client = getTestClient({
      rowCount: 0,
    });
    let result = await initiateMessageProcessing(
      { ...storedMessage },
      client,
      settings,
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
      settings,
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
      settings,
    );
    expect(result).toBe(true);
  });

  test.each([
    ['2023-01-18T21:02:27.000Z', null],
    [null, '2023-01-18T21:02:27.000Z'],
  ])(
    'it verifies that it updates the message properties when it was processed',
    async (processed, abandoned) => {
      // Arrange
      const client = getTestClient({
        rowCount: 1,
        rows: [
          {
            started_attempts: 4,
            finished_attempts: 3,
            processed_at: processed
              ? new Date('2023-01-18T21:02:27.000Z')
              : null,
            abandoned_at: abandoned
              ? new Date('2023-01-18T21:02:27.000Z')
              : null,
          },
        ],
      });
      const msg = { ...storedMessage };

      // Act
      const result = await initiateMessageProcessing(msg, client, settings);

      // Assert
      expect(result).toBe(
        processed ? 'ALREADY_PROCESSED' : 'ABANDONED_MESSAGE',
      );
      expect(msg).toMatchObject<
        Pick<
          StoredTransactionalMessage,
          'startedAttempts' | 'finishedAttempts' | 'processedAt' | 'abandonedAt'
        >
      >({
        startedAttempts: 4,
        finishedAttempts: 3,
        processedAt: processed ? '2023-01-18T21:02:27.000Z' : null,
        abandonedAt: abandoned ? '2023-01-18T21:02:27.000Z' : null,
      });
    },
  );

  test('it verifies that it updates the message properties when it was not processed', async () => {
    // Arrange
    const client = getTestClient({
      rowCount: 1,
      rows: [
        {
          started_attempts: 4,
          finished_attempts: 3,
          processed_at: null,
          abandoned_at: null,
        },
      ],
    });
    const msg = { ...storedMessage };

    // Act
    const result = await initiateMessageProcessing(msg, client, settings);

    // Assert
    expect(result).toBe(true);
    expect(msg).toMatchObject<
      Pick<
        StoredTransactionalMessage,
        'startedAttempts' | 'finishedAttempts' | 'processedAt' | 'abandonedAt'
      >
    >({
      startedAttempts: 4,
      finishedAttempts: 3,
      processedAt: null,
      abandonedAt: null,
    });
  });
});

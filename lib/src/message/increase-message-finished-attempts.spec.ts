/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { ListenerConfig } from '../common/base-config';
import { DatabaseClient } from '../common/database';
import { increaseMessageFinishedAttempts } from './increase-message-finished-attempts';
import { StoredTransactionalMessage } from './transactional-message';

const config = {
  settings: {
    dbSchema: 'test_schema',
    dbTable: 'test_table',
  },
} as ListenerConfig;

describe('increaseMessageFinishedAttempts', () => {
  it('The increase finished attempts logic increments the finished_attempts by one', async () => {
    // Arrange
    const client = {
      query: jest.fn().mockResolvedValue({
        rowCount: 0,
        rows: [],
      }),
    } as unknown as DatabaseClient;
    const storedMessage = { id: 'message-id' } as StoredTransactionalMessage;

    // Act
    await increaseMessageFinishedAttempts(storedMessage, client, config);

    // Assert
    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining(
        'UPDATE test_schema.test_table SET finished_attempts = finished_attempts + 1 WHERE id = $1',
      ),
      [storedMessage.id],
    );
  });
});

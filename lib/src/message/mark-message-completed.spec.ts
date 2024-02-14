/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { DatabaseClient } from '../common/database';
import { ListenerConfig } from '../common/listener-config';
import { markMessageCompleted } from './mark-message-completed';
import { StoredTransactionalMessage } from './transactional-message';

const config = {
  settings: {
    dbSchema: 'test_schema',
    dbTable: 'test_table',
  },
} as ListenerConfig;

describe('markMessageCompleted', () => {
  it('should call query with the correct parameters', async () => {
    // Arrange
    const client = {
      query: jest.fn().mockResolvedValue({
        rowCount: 0,
        rows: [],
      }),
    } as unknown as DatabaseClient;
    const storedMessage = { id: 'message-id' } as StoredTransactionalMessage;

    // Act
    await markMessageCompleted(storedMessage, client, config);

    // Assert
    expect(client.query).toHaveBeenCalledWith(
      `UPDATE ${config.settings.dbSchema}.${config.settings.dbTable} SET processed_at = NOW(), finished_attempts = finished_attempts + 1 WHERE id = $1`,
      [storedMessage.id],
    );
  });
});

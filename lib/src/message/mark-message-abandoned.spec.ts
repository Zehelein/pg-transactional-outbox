import { ListenerConfig } from '../../dist';
import { DatabaseClient } from '../common/database';
import { markMessageAbandoned } from './mark-message-abandoned';
import { StoredTransactionalMessage } from './transactional-message';

describe('markMessageAbandoned', () => {
  it('should mark the message as abandoned', async () => {
    // Arrange
    const mockMessage = {
      id: 'c68c0bef-b31e-42f9-ac1b-1c38f9597972',
    } as unknown as StoredTransactionalMessage;
    const mockClient = {
      query: jest.fn(),
    } as unknown as DatabaseClient;
    const mockConfig = {
      settings: {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
      },
    } as unknown as ListenerConfig;

    // Act
    await markMessageAbandoned(mockMessage, mockClient, mockConfig);

    // Assert
    expect(mockClient.query).toHaveBeenCalledWith(
      /* sql */ `UPDATE test_schema.test_table SET abandoned_at = NOW(), finished_attempts = finished_attempts + 1 WHERE id = $1;`,
      [mockMessage.id],
    );
  });
});

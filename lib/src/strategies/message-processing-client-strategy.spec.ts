import { getDisabledLogger, getInMemoryLogger } from '../common/logger';
import { InboxMessage } from '../common/message';
import { InboxConfig } from '../inbox/inbox-listener';
import { defaultMessageProcessingClientStrategy } from './message-processing-client-strategy';

let errorHandler: (err: Error) => void;
jest.mock('pg', () => {
  return {
    Pool: jest.fn().mockImplementation(() => ({
      on: jest.fn((event: 'error', callback: (err: Error) => void) => {
        errorHandler = callback;
      }),
      connect: jest.fn().mockResolvedValue({
        on: jest.fn(),
        query: jest.fn(),
        release: jest.fn(),
        listeners: jest.fn().mockReturnValue([]),
        escapeLiteral: (p: string) => p,
      }),
    })),
  };
});

describe('defaultMessageProcessingClientStrategy', () => {
  it('should return a DB client', async () => {
    const strategy = defaultMessageProcessingClientStrategy(
      {} as InboxConfig,
      getDisabledLogger(),
    );
    const client = await strategy.getClient({ id: '1' } as InboxMessage);
    expect(client.escapeLiteral('x')).toBe('x');
  });

  it('should log an error when the client encounters an error', async () => {
    // Arrange
    const [logger, logs] = getInMemoryLogger('test');
    const strategy = defaultMessageProcessingClientStrategy(
      {} as InboxConfig,
      logger,
    );
    const error = new Error('unit test');

    // Act
    await strategy.getClient({ id: '1' } as InboxMessage);

    // Assert
    expect(errorHandler).toBeDefined();
    errorHandler(error);
    expect(logs[0].args[0]).toBe(error);
  });
});

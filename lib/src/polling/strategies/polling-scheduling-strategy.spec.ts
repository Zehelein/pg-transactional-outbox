import { PollingConfig } from '../config';
import { defaultPollingListenerSchedulingStrategy } from './polling-scheduling-strategy';

describe('defaultPollingListenerSchedulingStrategy', () => {
  it('should return a scheduler with the configured polling interval', async () => {
    // Arrange
    const mockConfig = {
      settings: {
        nextMessagesPollingInterval: 200,
      },
    } as PollingConfig;

    // Act
    const result = await defaultPollingListenerSchedulingStrategy(mockConfig)();

    // Assert
    expect(result).toBe(200);
  });

  it('should use a default interval if not configured', async () => {
    // Arrange
    const mockConfig = {
      settings: {},
    } as PollingConfig;

    // Act
    const result = await defaultPollingListenerSchedulingStrategy(mockConfig)();

    // Assert
    expect(result).toBe(500);
  });
});

import { PollingListenerConfig } from '../config';
import { defaultPollingListenerBatchSizeStrategy } from './batch-size-strategy';

describe('defaultPollingListenerBatchSizeStrategy', () => {
  it('should return the configured batch size', async () => {
    // Arrange
    const mockConfig = {
      settings: {
        nextMessagesBatchSize: 3,
      },
    } as PollingListenerConfig;
    const strategy = defaultPollingListenerBatchSizeStrategy(mockConfig);

    // Act and assert
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(3);
    expect(await strategy(1)).toBe(3);
  });

  it('should increment the default size of 5 if not configured', async () => {
    // Arrange
    const mockConfig = {
      settings: {},
    } as PollingListenerConfig;
    const strategy = defaultPollingListenerBatchSizeStrategy(mockConfig);

    // Act and assert
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(5);
    expect(await strategy(1)).toBe(5);
  });
});

import { FullPollingListenerConfig } from '../config';
import { defaultPollingListenerBatchSizeStrategy } from './batch-size-strategy';

describe('defaultPollingListenerBatchSizeStrategy', () => {
  it('should return a batch size of one for the first 3 attempts and then the full size of 3', async () => {
    // Arrange
    const mockConfig = {
      settings: {
        nextMessagesBatchSize: 3,
      },
    } as FullPollingListenerConfig;
    const strategy = defaultPollingListenerBatchSizeStrategy(mockConfig);

    // Act and assert
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(1);
    expect(await strategy(1)).toBe(3);
    expect(await strategy(1)).toBe(3);
  });
});

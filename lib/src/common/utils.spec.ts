import inspector from 'inspector';
import { Pool } from 'pg';
import { getDisabledLogger } from './logger';
import { executeTransaction, sleep } from './utils';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000);
}

describe('Utils Unit Tests', () => {
  const logger = getDisabledLogger();
  describe('sleep', () => {
    it('should sleep for the given amount of milliseconds', async () => {
      // Arrange
      const sleepTime = 1000;
      const startTime = Date.now();

      // Act
      await sleep(sleepTime);

      // Assert
      const endTime = Date.now();
      expect(endTime - startTime).toBeGreaterThanOrEqual(sleepTime);
    });
  });

  describe('executeTransaction', () => {
    let pool: Pool;
    let callback: jest.Mock;

    beforeEach(() => {
      pool = {
        connect: jest.fn().mockResolvedValue({
          on: jest.fn(),
          query: jest.fn(),
          release: jest.fn(),
          listeners: jest.fn().mockReturnValue([]),
        }),
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any;
      callback = jest.fn().mockResolvedValue('SELECT 1;');
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should open a transaction and execute the callback', async () => {
      // Act
      await executeTransaction(pool, callback, logger);

      // Assert
      expect(pool.connect).toHaveBeenCalled();
      expect(callback).toHaveBeenCalled();
    });

    it('should return the result of the callback', async () => {
      // Act
      const result = await executeTransaction(pool, callback, logger);

      // Assert
      expect(result).toBe('SELECT 1;');
    });

    it('should commit the transaction if the callback resolves', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(pool, callback, logger);

      // Assert
      expect(client.query).toHaveBeenCalledWith('COMMIT');
      expect(client.release).toHaveBeenCalled();
    });

    it('should rollback the transaction if the callback throws', async () => {
      // Arrange
      callback.mockRejectedValue(new Error('Callback error'));
      const client = await pool.connect();

      // Act + Assert
      await expect(
        executeTransaction(pool, callback, logger),
      ).rejects.toThrow();
      expect(client.query).toHaveBeenCalledWith('ROLLBACK');
      expect(client.release).toHaveBeenCalled();
    });
  });
});

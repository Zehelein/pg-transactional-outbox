import inspector from 'inspector';
import { Pool } from 'pg';
import { getDisabledLogger } from './logger';
import {
  IsolationLevel,
  awaitWithTimeout,
  executeTransaction,
  sleep,
} from './utils';

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

  describe('awaitWithTimeout', () => {
    it('should resolve the promise before timeout', async () => {
      const expectedResult = 'expected result';
      const fastPromise = () => Promise.resolve(expectedResult);

      await expect(awaitWithTimeout(fastPromise, 100)).resolves.toBe(
        expectedResult,
      );
    });

    it('should reject with a timeout error when the promise takes too long', async () => {
      const slowPromise = async () => sleep(400);
      const failureMessage = 'Promise timed out!';

      await expect(
        awaitWithTimeout(slowPromise, 200, failureMessage),
      ).rejects.toThrow(failureMessage);
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
      await executeTransaction(
        pool,
        callback,
        IsolationLevel.Serializable,
        logger,
      );

      // Assert
      expect(pool.connect).toHaveBeenCalled();
      expect(callback).toHaveBeenCalled();
    });

    it('should open the transaction for isolation level "read committed" correctly', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(
        pool,
        callback,
        IsolationLevel.ReadCommitted,
        logger,
      );

      // Assert
      expect(client.query).toHaveBeenCalledWith(
        'START TRANSACTION ISOLATION LEVEL READ COMMITTED',
      );
      expect(client.release).toHaveBeenCalled();
    });

    it('should open the transaction for isolation level "repeatable read" correctly', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(
        pool,
        callback,
        IsolationLevel.RepeatableRead,
        logger,
      );

      // Assert
      expect(client.query).toHaveBeenCalledWith(
        'START TRANSACTION ISOLATION LEVEL REPEATABLE READ',
      );
      expect(client.release).toHaveBeenCalled();
    });

    it('should open the transaction for isolation level "serializable" correctly', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(
        pool,
        callback,
        IsolationLevel.Serializable,
        logger,
      );

      // Assert
      expect(client.query).toHaveBeenCalledWith(
        'START TRANSACTION ISOLATION LEVEL SERIALIZABLE',
      );
      expect(client.release).toHaveBeenCalled();
    });

    it('should open the transaction for a "not defined" isolation level correctly', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(pool, callback);

      // Assert
      expect(client.query).toHaveBeenCalledWith('BEGIN');
      expect(client.release).toHaveBeenCalled();
    });

    it('should open a default transaction for an invalid isolation level', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(
        pool,
        callback,
        'DROP TABLE outbox; --' as IsolationLevel,
      );

      // Assert
      expect(client.query).toHaveBeenCalledWith('BEGIN');
      expect(client.release).toHaveBeenCalled();
    });

    it('should return the result of the callback', async () => {
      // Act
      const result = await executeTransaction(
        pool,
        callback,
        IsolationLevel.Serializable,
        logger,
      );

      // Assert
      expect(result).toBe('SELECT 1;');
    });

    it('should commit the transaction if the callback resolves', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(
        pool,
        callback,
        IsolationLevel.Serializable,
        logger,
      );

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
        executeTransaction(pool, callback, IsolationLevel.Serializable, logger),
      ).rejects.toThrow();
      expect(client.query).toHaveBeenCalledWith('ROLLBACK');
      expect(client.release).toHaveBeenCalled();
    });
  });
});

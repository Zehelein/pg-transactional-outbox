import inspector from 'inspector';
import { Client, Pool } from 'pg';
import { getInMemoryLogger } from './logger';
import {
  IsolationLevel,
  awaitWithTimeout,
  executeTransaction,
  getClient,
  justDoIt,
  processPool,
  sleep,
} from './utils';

const isDebugMode = (): boolean => inspector.url() !== undefined;
if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(7_000);
}

describe('Utils Unit Tests', () => {
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

    it('should reject with a timeout error when the promise takes too long and show the default message', async () => {
      const slowPromise = async () => sleep(400);

      await expect(awaitWithTimeout(slowPromise, 200)).rejects.toThrow(
        'Timeout',
      );
    });
  });

  describe('executeTransaction', () => {
    let pool: Pool;
    let callback: jest.Mock;

    beforeEach(() => {
      pool = {
        connect: jest.fn().mockResolvedValue(
          (() => {
            const client = Object.create(Client.prototype);
            client.on = jest.fn();
            client.query = jest.fn();
            client.release = jest.fn();
            client.listeners = jest.fn().mockReturnValue([]);
            client.removeAllListeners = jest.fn();
            return client;
          })(),
        ),
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any;
      callback = jest.fn().mockResolvedValue('SELECT 1;');
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should open a transaction and execute the callback', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(client, callback, IsolationLevel.Serializable);

      // Assert
      expect(pool.connect).toHaveBeenCalled();
      expect(callback).toHaveBeenCalled();
    });

    it('should open the transaction for isolation level "read committed" correctly', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(client, callback, IsolationLevel.ReadCommitted);

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
      await executeTransaction(client, callback, IsolationLevel.RepeatableRead);

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
      await executeTransaction(client, callback, IsolationLevel.Serializable);

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
      await executeTransaction(client, callback);

      // Assert
      expect(client.query).toHaveBeenCalledWith('BEGIN');
      expect(client.release).toHaveBeenCalled();
    });

    it('should open a default transaction for an invalid isolation level', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(
        client,
        callback,
        'DROP TABLE outbox; --' as IsolationLevel,
      );

      // Assert
      expect(client.query).toHaveBeenCalledWith('BEGIN');
      expect(client.release).toHaveBeenCalled();
    });

    it('should return the result of the callback', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      const result = await executeTransaction(
        client,
        callback,
        IsolationLevel.Serializable,
      );

      // Assert
      expect(result).toBe('SELECT 1;');
    });

    it('should return the result of the callback even if the client is a client without a release function', async () => {
      // Arrange
      const client = {
        on: jest.fn(),
        query: jest.fn(),
        listeners: jest.fn().mockReturnValue([]),
        removeAllListeners: jest.fn(),
      } as unknown as Client;

      // Act
      const result = await executeTransaction(
        client,
        callback,
        IsolationLevel.Serializable,
      );

      // Assert
      expect(result).toBe('SELECT 1;');
    });

    it('should commit the transaction if the callback resolves', async () => {
      // Arrange
      const client = await pool.connect();

      // Act
      await executeTransaction(client, callback, IsolationLevel.Serializable);

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
        executeTransaction(client, callback, IsolationLevel.Serializable),
      ).rejects.toThrow();
      expect(client.query).toHaveBeenCalledWith('ROLLBACK');
      expect(client.release).toHaveBeenCalled();
    });

    it('should rollback the transaction if the callback throws and log any rollback error as inner error', async () => {
      // Arrange
      callback.mockRejectedValue(new Error('Callback error'));
      const client = await pool.connect();
      client.query = jest
        .fn()
        .mockResolvedValueOnce(Promise.resolve()) // start transaction
        .mockReturnValueOnce(Promise.reject(new Error('Inner error'))); // rollback

      // Act + Assert
      await expect(
        executeTransaction(client, callback, IsolationLevel.Serializable),
      ).rejects.toThrow('Callback error');
      expect(client.query).toHaveBeenCalledWith('ROLLBACK');
      expect(client.release).toHaveBeenCalled();
    });

    it('should rollback the transaction if the callback throws and log any rollback error as inner error even when a client without release is used', async () => {
      // Arrange
      callback.mockRejectedValue(new Error('Callback error'));
      const client = {
        on: jest.fn(),
        query: jest
          .fn()
          .mockResolvedValueOnce(Promise.resolve()) // start transaction
          .mockReturnValueOnce(Promise.reject(new Error('Inner error'))), // rollback
        listeners: jest.fn().mockReturnValue([]),
      } as unknown as Client;

      // Act + Assert
      await expect(
        executeTransaction(client, callback, IsolationLevel.Serializable),
      ).rejects.toThrow('Callback error');
    });
  });

  describe('getClient', () => {
    it('should return a client from the pool', async () => {
      let errorCallback: undefined | ((err: Error) => void);
      const client = {
        listeners: () => ({ length: 0 }),
        on: (_event: 'error', callback: (err: Error) => void) => {
          errorCallback = callback;
        },
        removeAllListeners: jest.fn(),
        connection: {
          removeAllListeners: jest.fn(),
          on: jest.fn(),
        },
      };
      const pool = {
        connect: jest.fn(() => client),
      } as unknown as Pool;
      const [logger, logs] = getInMemoryLogger('test');
      const returnedClient = await getClient(pool, logger);
      expect(returnedClient).toBeDefined();
      expect(errorCallback).toBeDefined();
      const error = new Error('test...');
      errorCallback?.(error);
      expect(logs[0].args[0]).toBeInstanceOf(Error);
      expect(logs[0].args[1]).toBe('PostgreSQL client error');
    });
  });

  describe('processPool', () => {
    it('should process the items in the pool and get fresh ones when the pool items are finished', async () => {
      const processingPool = new Set<Promise<void>>();
      let i = 1;
      const getItem = (ms: number) => {
        const item = sleep(ms);
        (item as any).name = `item${i++} - ${ms}ms`;
        return item;
      };
      let calls = 0;
      const getNextBatch = jest.fn(async () => {
        switch (calls++) {
          case 0: //                              started  | finished after
            return [getItem(50), getItem(50)]; // 10ms     | 60ms
          case 1:
            return [getItem(150)]; //             70ms     | 220ms (this item and a 100ms sleep)
          case 2:
            return [getItem(40)]; //              180ms    | 220mx (100ms sleep is done)
          case 3:
            return [getItem(50), getItem(50)]; // 230ms    | 280ms
          case 4:
            return []; //                         290ms    | 400ms (100ms sleep)
          case 5:
            return [getItem(50), getItem(50)]; // 410ms    | 460ms
          default:
            return []; //                         should not be started
        }
      });
      const getBatchSize = jest.fn(async () => {
        // give some time to align processing end times to match
        await sleep(10);
        return 2;
      });
      const signal = { stopped: false };
      processPool(processingPool, getNextBatch, getBatchSize, signal, 100);
      await sleep(420); // wait until the actual items are started
      signal.stopped = true;
      await sleep(80); // wait until the last items are finished

      expect(getNextBatch).toHaveBeenCalledTimes(6);
      expect(getBatchSize).toHaveBeenCalledTimes(6);
      expect(processingPool.size).toBe(0);
    });
  });

  describe('justDoIt', () => {
    it('should execute the given function when no error is thrown', async () => {
      const it = jest.fn();
      await justDoIt(it);
      expect(it).toHaveBeenCalled();
    });
    it('should execute the given function and swallow the error', async () => {
      const it = jest.fn(() => {
        throw new Error('it');
      });
      await justDoIt(it);
      expect(it).toHaveBeenCalled();
    });
  });
});

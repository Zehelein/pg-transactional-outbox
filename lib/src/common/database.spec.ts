import { Client, PoolClient } from 'pg';
import { isPgSerializationError, releaseIfPoolClient } from './database';

describe('Database Unit Tests', () => {
  describe('releaseIfPoolClient', () => {
    it('should call release if it is a pool client', () => {
      // Arrange
      const client = new Client() as unknown as PoolClient;
      client.release = jest.fn();

      // Act
      releaseIfPoolClient(client);

      // Assert
      expect(client.release).toHaveBeenCalled();
    });

    it('should call release if the object has a release function which is important if the PoolClient is a different hoisted version', () => {
      // Arrange
      const hoistedPoolClient = Object.create({});
      hoistedPoolClient.release = jest.fn();

      // Act
      releaseIfPoolClient(hoistedPoolClient);

      // Assert
      expect(hoistedPoolClient.release).toHaveBeenCalled();
    });

    it('should not throw an error if the client is no pool client', () => {
      // Arrange
      const client = new Client();

      // Act
      expect(() => releaseIfPoolClient(client)).not.toThrow();
    });
  });

  describe('isPgSerializationError', () => {
    it('returns true for serialization errors', () => {
      const error = { code: '40001' };
      expect(isPgSerializationError(error)).toBe(true);
    });

    it('returns true for deadlock errors', () => {
      const error = { code: '40P01' };
      expect(isPgSerializationError(error)).toBe(true);
    });

    it('returns false for other errors', () => {
      const error = { code: '42601' }; // SQL syntax error
      expect(isPgSerializationError(error)).toBe(false);
    });

    it('returns false for undefined error', () => {
      const error = undefined;
      expect(isPgSerializationError(error)).toBe(false);
    });

    it('returns false for null error', () => {
      const error = null;
      expect(isPgSerializationError(error)).toBe(false);
    });

    it('returns false for non-object error', () => {
      const error = 'not an object';
      expect(isPgSerializationError(error)).toBe(false);
    });

    it('returns false for error without code', () => {
      const error = {};
      expect(isPgSerializationError(error)).toBe(false);
    });
  });
});

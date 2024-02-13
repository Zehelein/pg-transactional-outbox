import { Client, PoolClient } from 'pg';
import { releaseIfPoolClient } from './database';

describe('Database Unit Tests', () => {
  it('should call release if it is a pool client', () => {
    // Arrange
    const client = new Client() as unknown as PoolClient;
    client.release = jest.fn();

    // Act
    releaseIfPoolClient(client);

    // Assert
    expect(client.release).toHaveBeenCalled();
  });

  it('should not throw an error if the client is no pool client', () => {
    // Arrange
    const client = new Client();

    // Act
    expect(() => releaseIfPoolClient(client)).not.toThrow();
  });
});

import { TransactionalMessage } from '../message/transactional-message';
import {
  MessageError,
  TransactionalOutboxInboxError,
  ensureExtendedError,
} from './error';

describe('Error Unit Tests', () => {
  describe('TransactionalOutboxInboxError', () => {
    it('should create an instance of TransactionalOutboxInboxError', () => {
      const message = 'This is a test message';
      const innerError = new Error('This is an inner error');
      const messageError = new TransactionalOutboxInboxError(
        message,
        'NO_MESSAGE_HANDLER_REGISTERED',
        innerError,
      );

      expect(messageError).toBeInstanceOf(TransactionalOutboxInboxError);
      expect(messageError.message).toBe(message);
      expect(messageError.innerError).toBe(innerError);
      expect(messageError.errorCode).toBe('NO_MESSAGE_HANDLER_REGISTERED');
    });
  });
  describe('MessageError', () => {
    it('should create an instance of MessageError', () => {
      const message = 'This is a test message';
      const messageObject = {
        id: 'e9fa5ba6-917e-4ac0-850b-3acddf37b3d5',
        message: 'Test message',
        aggregateType: 'test-aggregate',
        aggregateId: '123',
        messageType: 'test-event',
        payload: { test: true },
        createdAt: '2023-01-18T21:02:27.000Z',
      };
      const innerError = new Error('This is an inner error');
      const messageError = new MessageError(
        message,
        'MESSAGE_HANDLING_FAILED',
        messageObject,
        innerError,
      );

      expect(messageError).toBeInstanceOf(MessageError);
      expect(messageError.message).toBe(message);
      expect(messageError.messageObject).toBe(messageObject);
      expect(messageError.innerError).toBe(innerError);
      expect(messageError.errorCode).toBe('MESSAGE_HANDLING_FAILED');
    });
  });

  describe('ensureExtendedError', () => {
    it('should return the input error if it is an instance of TransactionalOutboxInboxError', () => {
      // Arrange
      const inputError = new TransactionalOutboxInboxError(
        'This is an error',
        'DB_ERROR',
      );

      // Act
      const error = ensureExtendedError(inputError, 'DB_ERROR');

      // Assert
      expect(error).toBe(inputError);
    });

    it('should return a MessageError when a message is given as input and add the error as innerError.', () => {
      // Arrange
      const input = new Error('This is an error');
      const message = { id: 'test-message' } as TransactionalMessage;

      // Act
      const error = ensureExtendedError(input, 'DB_ERROR', message);

      // Assert
      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(MessageError);
      expect(error.message).toBe(input.message);
      expect((error as MessageError<TransactionalMessage>).messageObject).toBe(
        message,
      );
      expect(error.errorCode).toBe('DB_ERROR');
      expect(error.innerError).toBe(input);
    });

    it('should return the error containing now the fallback error code if the input is an instance of Error', () => {
      // Arrange
      const input = new Error('This is an error');

      // Act
      const error = ensureExtendedError(input, 'DB_ERROR');

      // Assert
      expect(error).toBeInstanceOf(Error);
      expect(error).not.toBeInstanceOf(TransactionalOutboxInboxError);
      expect(error.message).toBe(input.message);
      expect(error.errorCode).toBe('DB_ERROR');
      expect(error.innerError).toBeUndefined();
    });

    it('should return a new error containing the fallback error code if the input is not an instance of Error', () => {
      // Arrange
      const input = 'This is not an error object';

      // Act
      const error = ensureExtendedError(input, 'DB_ERROR');

      // Assert
      expect(error).toBeInstanceOf(Error);
      expect(error).not.toBeInstanceOf(TransactionalOutboxInboxError);
      expect(error.message).toBe(input);
      expect(error.innerError).toBeUndefined();
    });
  });
});

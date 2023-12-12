import { MessageError, ensureError } from './error';

describe('Error Unit Tests', () => {
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
      const messageError = new MessageError(message, messageObject, innerError);

      expect(messageError).toBeInstanceOf(MessageError);
      expect(messageError.message).toBe(message);
      expect(messageError.messageObject).toBe(messageObject);
      expect(messageError.innerError).toBe(innerError);
    });
  });
  describe('ensureError', () => {
    it('should return the input error if it is an instance of Error', () => {
      // Arrange
      const inputError = new Error('This is an error');

      // Act
      const error = ensureError(inputError);

      // Assert
      expect(error).toBe(inputError);
    });

    it('should return a new Error if the input is not an instance of Error', () => {
      // Arrange
      const input = 'This is not an error';

      // Act
      const error = ensureError(input);

      // Assert
      expect(error).toEqual(new Error(input));
    });
  });
});

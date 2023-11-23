import { ensureError } from './error';

describe('Error Unit Tests', () => {
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

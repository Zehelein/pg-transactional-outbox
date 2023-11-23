/* eslint-disable @typescript-eslint/no-empty-function */
import { createAcknowledgeManager } from './acknowledge-manager';

const mockLogger = {
  fatal: () => {},
  error: () => {},
  warn: () => {},
  info: () => {},
  debug: () => {},
  trace: jest.fn(),
  silent: () => {},
  level: 'info',
};

const acknowledgeLsn = (lsn: string): void => {
  mockLogger.trace(`Acknowledged LSN: ${lsn}`);
};

describe('Acknowledge Manager', () => {
  beforeEach(() => {
    mockLogger.trace.mockClear();
  });

  test('should start and finish processing LSNs when they are in order', () => {
    // Arrange
    const { startProcessingLSN, finishProcessingLSN } =
      createAcknowledgeManager(acknowledgeLsn, mockLogger);

    const lsn1 = '0/16B6E58';
    const lsn2 = '0/16B6E60';

    // Act and Assert
    startProcessingLSN(lsn1);
    startProcessingLSN(lsn2);

    finishProcessingLSN(lsn1);
    expect(mockLogger.trace).toHaveBeenCalledTimes(3);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn1} - waiting for acknowledgement.`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Acknowledging LSN up to ${lsn1}`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(`Acknowledged LSN: ${lsn1}`);
    mockLogger.trace.mockClear();

    finishProcessingLSN(lsn2);
    expect(mockLogger.trace).toHaveBeenCalledTimes(3);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn2} - waiting for acknowledgement.`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Acknowledging LSN up to ${lsn2}`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(`Acknowledged LSN: ${lsn2}`);
  });

  test('should acknowledge the correct LSN when they are finishing in mixed order', () => {
    // Arrange
    const { startProcessingLSN, finishProcessingLSN } =
      createAcknowledgeManager(acknowledgeLsn, mockLogger);

    const lsn1 = '0/16B6E58';
    const lsn2 = '0/16B6E60';
    const lsn3 = '0/16B6E68';

    // Act and Assert
    startProcessingLSN(lsn1);
    startProcessingLSN(lsn2);
    startProcessingLSN(lsn3);

    // lsn2 is finished first, no acknowledgement should happen because it's not in order
    finishProcessingLSN(lsn2);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn2} - waiting for acknowledgement.`,
    );

    // lsn1 is finished, now lsn1 and lsn2 should be acknowledged
    finishProcessingLSN(lsn1);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn2} - waiting for acknowledgement.`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Acknowledging LSN up to ${lsn2}`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(`Acknowledged LSN: ${lsn2}`);

    // lsn3 is finished, should now also be acknowledged
    finishProcessingLSN(lsn3);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn3} - waiting for acknowledgement.`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Acknowledging LSN up to ${lsn3}`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(`Acknowledged LSN: ${lsn3}`);
  });

  test('should acknowledge the correct LSN when they are finishing in reverse order', () => {
    // Arrange
    const { startProcessingLSN, finishProcessingLSN } =
      createAcknowledgeManager(acknowledgeLsn, mockLogger);

    const lsn1 = '0/16B6E58';
    const lsn2 = '0/16B6E60';
    const lsn3 = '0/16B6E70';

    // Act and Assert
    startProcessingLSN(lsn1);
    startProcessingLSN(lsn2);
    startProcessingLSN(lsn3);

    finishProcessingLSN(lsn3);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn3} - waiting for acknowledgement.`,
    );

    finishProcessingLSN(lsn2);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn2} - waiting for acknowledgement.`,
    );

    finishProcessingLSN(lsn1);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn3} - waiting for acknowledgement.`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Acknowledging LSN up to ${lsn3}`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(`Acknowledged LSN: ${lsn3}`);
  });

  test('should acknowledge the correct LSN when processing and finishing is mixed', () => {
    // Arrange
    const { startProcessingLSN, finishProcessingLSN } =
      createAcknowledgeManager(acknowledgeLsn, mockLogger);

    const lsn1 = '0/16B6E40';
    const lsn2 = '0/16B6E50';
    const lsn3 = '0/16B6E60';
    const lsn4 = '0/16B6E70';
    const lsn5 = '0/16B6E80';
    const lsn6 = '0/16B6E90';

    // Act and Assert
    // Starting and finishing lsn1+2 and starting lsn3 in-between
    startProcessingLSN(lsn1);
    startProcessingLSN(lsn2);

    finishProcessingLSN(lsn2);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn2} - waiting for acknowledgement.`,
    );

    startProcessingLSN(lsn3);

    finishProcessingLSN(lsn1);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn2} - waiting for acknowledgement.`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Acknowledging LSN up to ${lsn2}`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(`Acknowledged LSN: ${lsn2}`);
    mockLogger.trace.mockClear();

    // Starting and finishing lsn4 message and finish the lsn3 from above
    startProcessingLSN(lsn4);
    finishProcessingLSN(lsn4);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn4} - waiting for acknowledgement.`,
    );

    finishProcessingLSN(lsn3);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn4} - waiting for acknowledgement.`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Acknowledging LSN up to ${lsn4}`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(`Acknowledged LSN: ${lsn4}`);
    mockLogger.trace.mockClear();

    // adding lsn6 and lsn5 out of order and finish them
    startProcessingLSN(lsn6);
    startProcessingLSN(lsn5);

    finishProcessingLSN(lsn6);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn6} - waiting for acknowledgement.`,
    );

    finishProcessingLSN(lsn5);
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Finished LSN ${lsn6} - waiting for acknowledgement.`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(
      `Acknowledging LSN up to ${lsn6}`,
    );
    expect(mockLogger.trace).toHaveBeenCalledWith(`Acknowledged LSN: ${lsn6}`);
  });

  test('should throw an error when trying to start processing the same LSN twice', () => {
    // Arrange
    const { startProcessingLSN } = createAcknowledgeManager(
      acknowledgeLsn,
      mockLogger,
    );
    const lsn1 = '0/16B6E58';

    // Act and Assert
    startProcessingLSN(lsn1);
    expect(() => startProcessingLSN(lsn1)).toThrow(
      `LSN ${lsn1} is already being processed.`,
    );
  });

  test('should throw an error when trying to finish processing an not yet started LSN', () => {
    // Arrange
    const { finishProcessingLSN } = createAcknowledgeManager(
      acknowledgeLsn,
      mockLogger,
    );
    const lsn1 = '0/16B6E58';

    // Act and Assert
    expect(() => finishProcessingLSN(lsn1)).toThrow(
      `LSN ${lsn1} was not registered as processing.`,
    );
  });
});

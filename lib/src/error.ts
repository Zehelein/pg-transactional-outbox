import { OutboxMessage } from './models';

export type ErrorType = 'transient_error' | 'permanent_error';

/** An error instance which holds an inner error and the processed message details */
export class MessageHandlingError extends Error {
  constructor(
    message: string,
    public innerError: Error,
    public receivedMessage: OutboxMessage,
  ) {
    super(message);
    this.name = this.constructor.name;
  }
}

/**
 * Returns the error as verified Error object or wraps the input as Error.
 * @param error The error variable to check
 * @returns The error if the input was already an error otherwise a wrapped error.
 */
export const ensureError = (error: unknown): Error => {
  if (error instanceof Error) {
    return error;
  }
  return new Error(String(error));
};

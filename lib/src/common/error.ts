import { OutboxMessage } from './message';

export type ErrorType = 'transient_error' | 'permanent_error';

/** An error that was raised when handling an outbox/inbox message. */
export class MessageError<T extends OutboxMessage> extends Error {
  constructor(
    message: string,
    public messageObject: T,
    public innerError?: Error,
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

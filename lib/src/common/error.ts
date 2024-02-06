import { TransactionalMessage } from '../message/transactional-message';

export type ErrorCode =
  | 'DB_ERROR'
  | 'MESSAGE_HANDLING_FAILED'
  | 'MESSAGE_ERROR_HANDLING_FAILED'
  | 'GIVING_UP_MESSAGE_HANDLING'
  | 'POISONOUS_MESSAGE'
  | 'CONFLICTING_MESSAGE_HANDLERS'
  | 'NO_MESSAGE_HANDLER_REGISTERED'
  | 'DISCRIMINATING_MUTEX_NOT_CONFIGURED'
  | 'LSN_ALREADY_PROCESSED'
  | 'LSN_NOT_PROCESSING'
  | 'LISTENER_STOPPED'
  | 'TIMEOUT'
  | 'MESSAGE_STORAGE_FAILED'
  | 'BATCH_PROCESSING_ERROR';

export interface ExtendedError extends Error {
  errorCode: ErrorCode;
  innerError?: Error;
}

/** An error that was raised from the transactional outbox/inbox library. Includes an error code. */
export class TransactionalOutboxInboxError
  extends Error
  implements ExtendedError
{
  public innerError?: Error;
  constructor(
    message: string,
    public errorCode: ErrorCode,
    innerError?: unknown,
  ) {
    super(message);
    this.name = this.constructor.name;
    this.innerError = ensureError(innerError);
  }
}

/** An error that was raised when handling an outbox/inbox message. */
export class MessageError<
  T extends TransactionalMessage,
> extends TransactionalOutboxInboxError {
  constructor(
    message: string,
    errorCode: ErrorCode,
    public messageObject: T,
    innerError?: unknown,
  ) {
    super(message, errorCode, innerError);
    this.name = this.constructor.name;
  }
}

/**
 * Returns the error as verified Error object or wraps the input as
 * ExtendedError with error code and potential innerError.
 * @param error The error variable to check
 * @returns The error if the input was already an error otherwise a wrapped error. Enriched with the error code property.
 */
export const ensureExtendedError = (
  error: unknown,
  fallbackErrorCode: ErrorCode,
): ExtendedError => {
  if (error instanceof TransactionalOutboxInboxError) {
    return error;
  }
  const err = ensureError(error) as ExtendedError;
  err.errorCode = fallbackErrorCode;
  return err;
};

const ensureError = (error: unknown): Error | undefined => {
  if (error === null || error === undefined) {
    return undefined;
  }
  if (error instanceof Error) {
    return error;
  }
  return new Error(String(error));
};

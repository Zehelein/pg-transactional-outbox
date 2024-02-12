import { DatabaseClient } from '../common/database';
import { ExtendedError } from '../common/error';
import { StoredTransactionalMessage } from '../message/transactional-message';

/**
 * Message handler for handling all aggregate types and message types.
 */
export interface GeneralMessageHandler {
  /**
   * Custom business logic to handle a message that was stored in the
   * transactional table. It is fine to throw an error if the message cannot be
   * processed.
   * @param message The message with the payload to handle.
   * @param client The database client that is part of a transaction to safely handle the message.
   * @throws If something failed and the message should NOT be acknowledged - throw an error.
   */
  handle: (
    message: StoredTransactionalMessage,
    client: DatabaseClient,
  ) => Promise<void>;

  /**
   * Custom (optional) business logic to handle an error that was caused by the
   * "handle" method. It is advised to not throw errors in this function for
   * most consistent error handling.
   * @param error The error that was thrown in the handle method.
   * @param message The message with the payload that was attempted to be handled. The finishedAttempts property includes the number of processing attempts - including the current one if DB retries management is activated.
   * @param client The database client that is part of a (new) transaction to safely handle the error.
   * @param retry True if the message should be retried again.
   */
  handleError?: (
    error: ExtendedError,
    message: StoredTransactionalMessage,
    client: DatabaseClient,
    retry: boolean,
  ) => Promise<void>;
}

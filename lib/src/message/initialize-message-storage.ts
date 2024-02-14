import { DatabaseClient } from '../common/database';
import { MessageError } from '../common/error';
import { ListenerConfig, ListenerSettings } from '../common/listener-config';
import { TransactionalLogger } from '../common/logger';
import { TransactionalMessage } from './transactional-message';

export interface MessageStorage {
  (message: TransactionalMessage, client: DatabaseClient): Promise<void>;
}

/**
 * Initialize the message storage to store outbox or inbox messages in the corresponding table.
 * @param config The configuration object that defines the values on how to connect to the database and general settings.
 * @param logger A logger instance for logging trace up to error logs
 * @returns Initializes the function to store the outbox or inbox message data to the database and provides the shutdown action.
 */
export const initializeMessageStorage = (
  {
    settings,
    outboxOrInbox,
  }: Pick<ListenerConfig, 'settings' | 'outboxOrInbox'>,
  logger: TransactionalLogger,
): MessageStorage => {
  /**
   * The function to store the message data to the database.
   * @param message The received message that should be stored as a outbox or inbox message
   * @param client A database client with an active transaction(!) can be provided. Otherwise
   * @throws Error if the message could not be stored
   */
  return async (
    message: TransactionalMessage,
    client: DatabaseClient,
  ): Promise<void> => {
    try {
      await insertMessage(message, client, settings, logger);
    } catch (err) {
      const messageError = new MessageError(
        `Could not store the ${outboxOrInbox} message with id ${message.id}`,
        'MESSAGE_STORAGE_FAILED',
        message,
        err,
      );
      logger.error(
        messageError,
        `Could not store the ${outboxOrInbox} message`,
      );
      throw messageError;
    }
  };
};

const insertMessage = async (
  message: TransactionalMessage,
  client: DatabaseClient,
  { dbSchema, dbTable }: ListenerSettings,
  logger: TransactionalLogger,
) => {
  const {
    id,
    aggregateType,
    aggregateId,
    messageType,
    segment,
    payload,
    metadata,
    concurrency,
    createdAt,
    lockedUntil,
  } = message;

  const insertValues = [
    id,
    aggregateType,
    aggregateId,
    messageType,
    segment,
    payload,
    metadata,
  ];

  let optionalPlaceholders = '';
  const addPlaceholder = () =>
    (optionalPlaceholders += `, $${insertValues.length}`);

  let optionalFields = '';
  const addOptionalFields = (name: string) => (optionalFields += `, ${name}`);
  if (concurrency) {
    insertValues.push(concurrency);
    addOptionalFields('concurrency');
    addPlaceholder();
  }
  if (createdAt) {
    insertValues.push(createdAt);
    addOptionalFields('created_at');
    addPlaceholder();
  }
  if (lockedUntil) {
    insertValues.push(lockedUntil);
    addOptionalFields('locked_until');
    addPlaceholder();
  }

  const messageResult = await client.query(
    /* sql */ `
    INSERT INTO ${dbSchema}.${dbTable}
      (id, aggregate_type, aggregate_id, message_type, segment, payload, metadata${optionalFields})
      VALUES ($1, $2, $3, $4, $5, $6, $7${optionalPlaceholders})
      ON CONFLICT (id) DO NOTHING`,
    insertValues,
  );
  if (!messageResult.rowCount || messageResult.rowCount < 1) {
    logger.warn(message, `The message with id ${id} already existed`);
  }
};

import { PoolClient } from 'pg';
import { v4 as uuid } from 'uuid';
import { MessageError } from '../common/error';
import { OutboxMessage } from '../common/message';
import { OutboxConfig } from './outbox-listener';

/**
 * Pre-configure the specific kind of outbox message to generate and receive a
 * function that can be called to actually store the specific outbox message.
 * @param aggregateType The aggregate root type that was responsible to create this message.
 * @param messageType The type of message that happened on the aggregate type.
 * @param config The configuration object that defines the outbox table schema.
 * @returns The function to store the outbox message data to the database.
 */
export const initializeOutboxMessageStorage = (
  aggregateType: string,
  messageType: string,
  { settings: { dbSchema, dbTable } }: Pick<OutboxConfig, 'settings'>,
) => {
  /**
   * Function to store the outbox message data to the database.
   * @param aggregateId The identifier of the aggregate.
   * @param payload The actual message payload that should be sent.
   * @param dbClient The database client that should have an active transaction to insert the outbox data along with the business logic transaction.
   * @param metadata Optional metadata that is/was used for the actual message transfer
   * @returns The outbox message data that was stored in the database.
   * @throws An error if the outbox message could not be created.
   */
  return async (
    aggregateId: string,
    payload: unknown,
    dbClient: PoolClient,
    metadata?: Record<string, unknown>,
  ): Promise<OutboxMessage> =>
    storeOutboxMessage(
      dbSchema,
      dbTable,
      aggregateId,
      aggregateType,
      messageType,
      payload,
      dbClient,
      metadata,
    );
};

/**
 * Create a general outbox message storage with only the DB settings to generate
 * and receive a function that can be called to actually store the specific
 * outbox message while supplying also the aggregate type and message type.
 * @param config The configuration object that defines the outbox table schema.
 * @returns The function to store the outbox message data to the database.
 */
export const initializeGeneralOutboxMessageStorage = ({
  settings: { dbSchema, dbTable },
}: Pick<OutboxConfig, 'settings'>) => {
  /**
   * Function to store the outbox message data to the database.
   * @param aggregateId The identifier of the aggregate.
   * @param aggregateType The aggregate root type that was responsible to create this message.
   * @param messageType The type of message that happened on the aggregate type.
   * @param payload The actual message payload that should be sent.
   * @param dbClient The database client that should have an active transaction to insert the outbox data along with the business logic transaction.
   * @param metadata Optional metadata that is/was used for the actual message transfer
   * @returns The outbox message data that was stored in the database.
   * @throws An error if the outbox message could not be created.
   */
  return async (
    aggregateId: string,
    aggregateType: string,
    messageType: string,
    payload: unknown,
    dbClient: PoolClient,
    metadata?: Record<string, unknown>,
  ): Promise<OutboxMessage> =>
    storeOutboxMessage(
      dbSchema,
      dbTable,
      aggregateId,
      aggregateType,
      messageType,
      payload,
      dbClient,
      metadata,
    );
};

const storeOutboxMessage = async (
  dbSchema: string,
  dbTable: string,
  aggregateId: string,
  aggregateType: string,
  messageType: string,
  payload: unknown,
  dbClient: PoolClient,
  metadata?: Record<string, unknown>,
): Promise<OutboxMessage> => {
  const outboxId = uuid();
  const outboxResult = await dbClient.query(
    /* sql*/ `
    INSERT INTO ${dbSchema}.${dbTable}
      (id, aggregate_type, aggregate_id, message_type, payload, metadata)
      VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id, created_at;`,
    [outboxId, aggregateType, aggregateId, messageType, payload, metadata],
  );
  const attemptedMessage = {
    aggregateType,
    aggregateId,
    messageType,
    payload,
    metadata,
    id: 'unknown',
    createdAt: 'unknown',
  };
  if (!outboxResult.rowCount || outboxResult.rowCount < 1) {
    throw new MessageError(
      'Could not insert the message into the outbox!',
      'OUTBOX_ERROR_STORAGE_FAILED',
      attemptedMessage,
    );
  }
  // Immediately delete the outbox entry - it was already written to the WAL
  await dbClient.query(
    /* sql*/ `
    DELETE FROM ${dbSchema}.${dbTable} WHERE id = $1;`,
    [outboxId],
  );
  return {
    ...attemptedMessage,
    id: outboxId,
    createdAt: outboxResult.rows[0].created_at,
  };
};

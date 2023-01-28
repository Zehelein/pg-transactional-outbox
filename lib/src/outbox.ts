import { v4 as uuid } from 'uuid';
import { ClientBase } from 'pg';
import { OutboxServiceConfig } from './outbox-service';
import { MessageError, OutboxMessage } from './models';

/**
 * Pre-configure the specific kind of outbox message to generate and receive a
 * function that can be called to actually store the specific outbox message.
 * @param aggregateType The aggregate root type that was responsible to create this message.
 * @param eventType The type of event that happened on the aggregate type.
 * @param config The configuration object that defines the outbox table schema.
 * @returns The function to store the outbox message data to the database.
 */
export const initializeOutboxMessageStorage = (
  aggregateType: string,
  eventType: string,
  { settings: { dbSchema, dbTable } }: Pick<OutboxServiceConfig, 'settings'>,
) => {
  /**
   * Function to store the outbox message data to the database.
   * @param aggregateId The identifier of the aggregate.
   * @param payload The actual message payload that should be sent.
   * @param dbClient The database client that should have an active transaction to insert the outbox data along with the business logic transaction.
   * @returns The outbox message data that was stored in the database.
   * @throws An error if the outbox message could not be created.
   */
  return async (
    aggregateId: string,
    payload: unknown,
    dbClient: ClientBase,
  ): Promise<OutboxMessage> => {
    const outboxId = uuid();
    const outboxResult = await dbClient.query(
      /* sql*/ `
      INSERT INTO ${dbSchema}.${dbTable}
        (id, aggregate_type, aggregate_id, event_type, payload)
        VALUES ($1, $2, $3, $4, $5)
      RETURNING id, created_at;`,
      [outboxId, aggregateType, aggregateId, eventType, payload],
    );
    const attemptedMessage = {
      aggregateType,
      aggregateId,
      eventType,
      payload,
      id: 'unknown',
      createdAt: 'unknown',
    };
    if (outboxResult.rowCount < 1) {
      throw new MessageError(
        'Could not insert the message into the outbox!',
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
};

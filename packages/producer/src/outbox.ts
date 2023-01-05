import { v4 as uuid } from 'uuid';
import { ClientBase } from 'pg';
import { Config } from './config';

/** The outbox message for storing it to the DB and receiving it back from the WAL */
export interface OutboxMessage {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  payload: unknown;
  createdAt: string;
}

/**
 * Pre-configure the specific kind of outbox message to generate and receive a
 * function that can be called to actually store the specific outbox message.
 * @param aggregateType The aggregate root type that was responsible to create this message.
 * @param eventType The type of event that happened on the aggregate type.
 * @param config The configuration object that defines the outbox table schema.
 * @returns The function to store the outbox message data to the database.
 */
export const outboxMessageStore = (
  aggregateType: string,
  eventType: string,
  { postgresOutboxSchema }: Pick<Config, 'postgresOutboxSchema'>,
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
      /*sql*/ `
      INSERT INTO ${postgresOutboxSchema}.outbox
        (id, aggregate_type, aggregate_id, event_type, payload)
        VALUES ($1, $2, $3, $4, $5)
      RETURNING id, created_at;`,
      [outboxId, aggregateType, aggregateId, eventType, payload],
    );
    if (outboxResult.rowCount < 1) {
      throw new Error('Could not insert the message into the outbox!');
    }
    // Immediately delete the outbox entry - it was already written to the WAL
    await dbClient.query(
      /*sql*/ `
      DELETE FROM ${postgresOutboxSchema}.outbox WHERE id = $1;`,
      [outboxId],
    );
    return {
      id: outboxId,
      aggregateType,
      aggregateId,
      eventType,
      payload,
      createdAt: outboxResult.rows[0].created_at,
    };
  };
};

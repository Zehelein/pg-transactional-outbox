/** The outbox message for storing it to the DB and receiving it back from the WAL */
export interface OutboxMessage {
  /** The unique identifier of the message. This is used to ensure a message is only processed once */
  id: string;
  /** The type of the aggregate root (in DDD context) to which this message is related */
  aggregateType: string;
  /** The unique identifier of the aggregate */
  aggregateId: string;
  /** The type name of the event or command */
  messageType: string;
  /** The message payload that provides the details for an event or instructions for a command */
  payload: unknown;
  /** Optional metadata that is/was used for the actual message transfer. */
  metadata?: Record<string, unknown>;
  /** The date and time in ISO 8601 "internet time" UTC format (e.g. "2023-10-17T11:48:14Z") when the message was created */
  createdAt: string;
}

/** The inbox message for storing it to the DB and receiving it back from the WAL */
export interface InboxMessage extends OutboxMessage {
  /** The number of times an inbox message was attempted to be processed. */
  startedAttempts: number;
  /** The number of times an inbox message was processed (successfully or with a caught error). */
  finishedAttempts: number;
  /** The date and time in ISO 8601 "internet time" UTC format (e.g. "2023-10-17T11:48:14Z") when the message was processed */
  processedAt: string | null;
}

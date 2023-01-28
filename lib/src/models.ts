/** The outbox message for storing it to the DB and receiving it back from the WAL */
export interface OutboxMessage {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  payload: unknown;
  createdAt: string;
}

/** The inbox message for storing it to the DB and receiving it back from the WAL */
export interface InboxMessage extends OutboxMessage {
  retries: number;
  processedAt: string | null;
}

/** An error that was raised when handling an outbox/inbox message. */
export class MessageError<T extends OutboxMessage> extends Error {
  public messageObject: T;

  constructor(message: string, messageObject: T) {
    super(message);
    this.name = 'MessageError';
    this.messageObject = messageObject;
  }
}

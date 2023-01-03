/** The inbox message for storing it to the DB and receiving it back from the WAL */
export interface InboxMessage {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  payload: unknown;
  createdAt: string;
  retries: number;
}

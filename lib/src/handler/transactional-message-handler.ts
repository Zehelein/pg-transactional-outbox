import { GeneralMessageHandler } from './general-message-handler';

/**
 * Message handler for a specific aggregate type and message type.
 */
export interface TransactionalMessageHandler extends GeneralMessageHandler {
  /** The aggregate root type */
  aggregateType: string;

  /** The name of the message created for the aggregate type. */
  messageType: string;
}

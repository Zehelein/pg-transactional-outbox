import { TransactionalOutboxInboxError } from '../common/error';
import { StoredTransactionalMessage } from '../message/transactional-message';
import { GeneralMessageHandler } from './general-message-handler';
import { TransactionalMessageHandler } from './transactional-message-handler';

export const messageHandlerSelector = (
  messageHandlers: TransactionalMessageHandler[] | GeneralMessageHandler,
): ((
  message: StoredTransactionalMessage,
) => GeneralMessageHandler | undefined) => {
  const handlerSelector = Array.isArray(messageHandlers)
    ? createMessageHandlerDict(messageHandlers)
    : messageHandlers;
  return (
    message: StoredTransactionalMessage,
  ): GeneralMessageHandler | undefined => {
    return selectMessageHandler(message, handlerSelector);
  };
};

const createMessageHandlerDict = (
  messageHandlers: TransactionalMessageHandler[],
): Record<string, TransactionalMessageHandler> => {
  const handlers: Record<string, TransactionalMessageHandler> = {};
  for (const handler of messageHandlers) {
    const key = getMessageHandlerKey(handler);
    if (key in handlers) {
      throw new TransactionalOutboxInboxError(
        `Only one message handler can handle one aggregate and message type. Multiple message handlers try to handle the aggregate type "${handler.aggregateType}" with the message type "${handler.messageType}".`,
        'CONFLICTING_MESSAGE_HANDLERS',
      );
    }
    handlers[key] = handler;
  }

  if (Object.keys(handlers).length === 0) {
    throw new TransactionalOutboxInboxError(
      'At least one message handler must be provided.',
      'NO_MESSAGE_HANDLER_REGISTERED',
    );
  }

  return handlers;
};

const selectMessageHandler = (
  message: StoredTransactionalMessage,
  handlerSelector:
    | Record<string, TransactionalMessageHandler>
    | GeneralMessageHandler,
): GeneralMessageHandler => {
  if (isGeneralMessageHandler(handlerSelector)) {
    return handlerSelector;
  } else {
    return handlerSelector[getMessageHandlerKey(message)];
  }
};

const getMessageHandlerKey = (
  h: Pick<StoredTransactionalMessage, 'aggregateType' | 'messageType'>,
) => `${h.aggregateType}@${h.messageType}`;

function isGeneralMessageHandler(
  handlerSelector:
    | Record<string, TransactionalMessageHandler>
    | GeneralMessageHandler,
): handlerSelector is GeneralMessageHandler {
  return !!(handlerSelector as GeneralMessageHandler).handle;
}

import { StoredTransactionalMessage } from '../message/transactional-message';
import { GeneralMessageHandler } from './general-message-handler';
import { messageHandlerSelector } from './message-handler-selector';
import { TransactionalMessageHandler } from './transactional-message-handler';

const message: StoredTransactionalMessage = {
  id: '2ddf1d9b-2c05-413e-bf52-d50a84cf3079',
  aggregateId: '1',
  aggregateType: 'movie',
  messageType: 'movie_created',
  payload: {},
  createdAt: new Date().toISOString(),
  concurrency: 'sequential',
  lockedUntil: new Date(Date.now() + 5 * 60 * 1000).toISOString(),
  startedAttempts: 0,
  finishedAttempts: 0,
  processedAt: null,
  abandonedAt: null,
};

describe('messageHandlerSelector', () => {
  const messageHandlers: TransactionalMessageHandler[] = [
    {
      aggregateType: 'movie',
      messageType: 'movie_created',
      handle: jest.fn(),
    },
    {
      aggregateType: 'movie',
      messageType: 'movie_updated',
      handle: jest.fn(),
    },
  ];

  const generalMessageHandler: GeneralMessageHandler = {
    handle: jest.fn(),
  };

  it('should return the general message handler for a message', () => {
    const selector = messageHandlerSelector(generalMessageHandler);
    const handler = selector(message);
    expect(handler).toBe(generalMessageHandler);
  });

  it('should return a transactional message handler if a matching one is found', () => {
    const selector = messageHandlerSelector(messageHandlers);
    const handler = selector(message);
    expect(handler).toBe(messageHandlers[0]);
  });

  it('should not return a transactional message handler if no matching one is found', () => {
    const selector = messageHandlerSelector(messageHandlers);
    const handler = selector({ ...message, aggregateType: 'not-found' });
    expect(handler).toBeUndefined();
  });

  it('should throw an error if multiple message handlers try to handle the same aggregate and message type', () => {
    const messageHandlersWithConflict: TransactionalMessageHandler[] = [
      ...messageHandlers,
      {
        aggregateType: 'movie',
        messageType: 'movie_created',
        handle: jest.fn(),
      },
    ];
    expect(() => messageHandlerSelector(messageHandlersWithConflict)).toThrow(
      'Only one message handler can handle one aggregate and message type. Multiple message handlers try to handle the aggregate type "movie" with the message type "movie_created".',
    );
  });

  it('should throw an error if no message handler is provided', () => {
    expect(() => messageHandlerSelector([])).toThrow(
      'At least one message handler must be provided.',
    );
  });
});

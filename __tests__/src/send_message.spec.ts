/* eslint-disable @typescript-eslint/no-empty-function */
import { ClientBase, Pool, PoolClient } from 'pg';
import { v4 as uuid } from 'uuid';
import {
  OutboxMessage,
  initializeOutboxService,
  initializeOutboxMessageStore,
  initializeInboxMessageStorage,
  initializeInboxService,
  InboxMessageHandler,
  InboxMessage,
  executeTransaction,
  setLogger,
} from 'pg-transactional-outbox';
import {
  defaultInboxConfig,
  defaultInboxServiceConfig,
  defaultLoginConnection,
  defaultOutboxConfig,
  defaultOutboxServiceConfig,
} from './test-utils/default-configs';

jest.setTimeout(300000);

const aggregateType = 'source_entity';
const eventType = 'source_entity_created';

const setupProducerAndConsumer = async (
  inboxMessageHandlers: InboxMessageHandler[],
) => {
  // omit logs for now but can be easily adjusted for testing logs
  const fakePinoLogger = {
    child: () => fakePinoLogger,
    debug: () => {},
    error: () => {},
    fatal: () => {},
    info: () => {},
    trace: () => {},
    warn: () => {},
    silent: () => {},
    level: 'silent',
  };
  setLogger(fakePinoLogger);

  // Inbox
  const { shutdown: inSrvShutdown } = await initializeInboxService(
    defaultInboxServiceConfig,
    inboxMessageHandlers,
  );
  const { storeInboxMessage, shutdown: inStoreShutdown } =
    await initializeInboxMessageStorage(defaultInboxConfig);

  // A simple in-process message sender
  const messageReceiver = async (message: OutboxMessage): Promise<void> => {
    await storeInboxMessage(message);
  };
  const messagePublisher = async (message: OutboxMessage): Promise<void> => {
    await messageReceiver(message);
  };

  // Outbox
  const { shutdown: outSrvShutdown } = await initializeOutboxService(
    defaultOutboxServiceConfig,
    messagePublisher,
  );
  const storeOutboxMessage = initializeOutboxMessageStore(
    aggregateType,
    eventType,
    defaultOutboxConfig,
  );

  return {
    storeOutboxMessage,
    shutdown: async () => {
      await inSrvShutdown();
      await inStoreShutdown();
      await outSrvShutdown();
    },
  };
};

describe('Sending a message from a producer to a consumer works', () => {
  let loginPool: Pool;

  beforeAll(() => {
    loginPool = new Pool(defaultLoginConnection);
  });

  afterAll(() => {
    loginPool.end();
  });

  test('A single message is sent and received', async () => {
    // Arrange
    const entityId = uuid();
    const content = `some content of entity with id ${entityId}`;
    let inboxMessageReceived: InboxMessage | undefined;
    const inboxMessageHandler = {
      aggregateType: aggregateType,
      eventType: eventType,
      handle: async (
        message: InboxMessage,
        _client: ClientBase,
      ): Promise<void> => {
        if (message.aggregateId !== entityId) {
          // WAL might report some older entry still
          return;
        }
        inboxMessageReceived = message;
      },
    };
    const { storeOutboxMessage, shutdown } = await setupProducerAndConsumer([
      inboxMessageHandler,
    ]);

    // Act
    await executeTransaction(loginPool, async (client: PoolClient) => {
      const entity = await client.query(
        `INSERT INTO public.source_entities (id, content) VALUES ($1, $2) RETURNING id, content;`,
        [entityId, content],
      );
      if (entity.rowCount !== 1) {
        throw Error(`Inserted ${entity.rowCount} rows`);
      }
      await storeOutboxMessage(entityId, entity.rows[0], client);
    });

    // Assert
    const timeout = Date.now() + 5000; // 5 secs
    while (!inboxMessageReceived && Date.now() < timeout) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    expect(inboxMessageReceived).toMatchObject({
      aggregateType,
      eventType,
      aggregateId: entityId,
      payload: { id: entityId, content },
    });
    await shutdown();
  });
});

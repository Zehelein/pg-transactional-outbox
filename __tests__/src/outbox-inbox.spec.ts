/* eslint-disable @typescript-eslint/no-empty-function */
import { resolve } from 'path';
import { ClientBase, Pool, PoolClient } from 'pg';
import { v4 as uuid } from 'uuid';
import {
  executeTransaction,
  InboxMessage,
  InboxMessageHandler,
  initializeInboxMessageStorage,
  initializeInboxService,
  initializeOutboxMessageStore,
  initializeOutboxService,
  logger,
  OutboxMessage,
  setLogger,
} from 'pg-transactional-outbox';
import { DockerComposeEnvironment } from 'testcontainers';
import { StartedDockerComposeEnvironment } from 'testcontainers/dist/docker-compose-environment/started-docker-compose-environment';
import { getConfigs, TestConfigs, setupTestDb, sleep } from './test-utils';

jest.setTimeout(300000);

const aggregateType = 'source_entity';
const eventType = 'source_entity_created';
const setupProducerAndConsumer = async (
  {
    inboxConfig,
    inboxServiceConfig,
    outboxConfig,
    outboxServiceConfig,
  }: TestConfigs,
  inboxMessageHandlers: InboxMessageHandler[],
  outboxPublisherWrapper?: (message: OutboxMessage) => boolean,
): Promise<
  [
    storeOutboxMessage: ReturnType<typeof initializeOutboxMessageStore>,
    shutdown: () => Promise<void>,
  ]
> => {
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
    inboxServiceConfig,
    inboxMessageHandlers,
  );
  const { storeInboxMessage, shutdown: inStoreShutdown } =
    await initializeInboxMessageStorage(inboxConfig);

  // A simple in-process message sender
  const messageReceiver = async (message: OutboxMessage): Promise<void> => {
    await storeInboxMessage(message);
  };
  const messagePublisher = async (message: OutboxMessage): Promise<void> => {
    if (
      outboxPublisherWrapper === undefined ||
      outboxPublisherWrapper(message)
    ) {
      await messageReceiver(message);
    }
  };

  // Outbox
  const { shutdown: outSrvShutdown } = await initializeOutboxService(
    outboxServiceConfig,
    messagePublisher,
  );
  const storeOutboxMessage = initializeOutboxMessageStore(
    aggregateType,
    eventType,
    outboxConfig,
  );

  return [
    storeOutboxMessage,
    async () => {
      await inSrvShutdown();
      await inStoreShutdown();
      await outSrvShutdown();
    },
  ];
};

const createContent = (id: string) => `Content for id ${id}`;

describe('Sending messages from a producer to a consumer works.', () => {
  let environment: StartedDockerComposeEnvironment;
  let loginPool: Pool;
  let configs: TestConfigs;
  let cleanup: { (): Promise<void> } | undefined = undefined;

  beforeAll(async () => {
    environment = await new DockerComposeEnvironment(
      resolve(__dirname, 'test-utils'),
      'docker-compose.yml',
    ).up();

    const postgresContainer = environment.getContainer('postgres');
    const port = postgresContainer.getMappedPort(5432);
    configs = getConfigs(port);
    await setupTestDb(configs);

    loginPool = new Pool(configs.loginConnection);
    loginPool.on('error', (err) => {
      logger().error(err, 'PostgreSQL pool error');
    });
  });

  afterEach(async () => {
    if (cleanup) {
      await cleanup();
    }
  });

  afterAll(async () => {
    await loginPool.end();
    await environment.down();
  });

  test('A single message is sent and received', async () => {
    // Arrange
    const entityId = uuid();
    const content = createContent(entityId);
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
    const [storeOutboxMessage, shutdown] = await setupProducerAndConsumer(
      configs,
      [inboxMessageHandler],
    );
    cleanup = shutdown;

    // Act
    await executeTransaction(loginPool, async (client: PoolClient) => {
      const entity = await client.query(
        `INSERT INTO public.source_entities (id, content) VALUES ($1, $2) RETURNING id, content;`,
        [entityId, content],
      );
      expect(entity.rowCount).toBe(1);
      await storeOutboxMessage(entityId, entity.rows[0], client);
    });

    // Assert
    const timeout = Date.now() + 5000; // 5 secs
    while (!inboxMessageReceived && Date.now() < timeout) {
      await sleep(100);
    }
    expect(inboxMessageReceived).toMatchObject({
      aggregateType,
      eventType,
      aggregateId: entityId,
      payload: { id: entityId, content },
    });
  });

  test('Ten messages are sent and received', async () => {
    // Arrange
    const receivedInboxMessages: InboxMessage[] = [];
    const inboxMessageHandler = {
      aggregateType: aggregateType,
      eventType: eventType,
      handle: async (
        message: InboxMessage,
        _client: ClientBase,
      ): Promise<void> => {
        receivedInboxMessages.push(message);
      },
    };
    const [storeOutboxMessage, shutdown] = await setupProducerAndConsumer(
      configs,
      [inboxMessageHandler],
    );
    cleanup = shutdown;
    // wait a bit so older WAL messages are received and then reset the array
    await sleep(500);
    receivedInboxMessages.length = 0;
    const ids = Array.from({ length: 10 }, () => uuid());

    // Act
    await executeTransaction(loginPool, async (client: PoolClient) => {
      await Promise.all(
        ids.map(async (id) => {
          const entity = await client.query(
            `INSERT INTO public.source_entities (id, content) VALUES ($1, $2) RETURNING id, content;`,
            [id, createContent(id)],
          );
          expect(entity.rowCount).toBe(1);
          await storeOutboxMessage(id, entity.rows[0], client);
        }),
      );
    });

    // Assert
    const timeout = Date.now() + 5000; // 5 secs
    while (receivedInboxMessages.length < 10 && Date.now() < timeout) {
      await sleep(100);
    }
    expect(receivedInboxMessages).toHaveLength(10);
    const compare = (
      { aggregateId: id1 }: { aggregateId: string },
      { aggregateId: id2 }: { aggregateId: string },
    ) => (id1 > id2 ? 1 : id2 > id1 ? -1 : 0);
    expect(receivedInboxMessages.sort(compare)).toMatchObject(
      ids
        .map((id) => ({
          aggregateType,
          eventType,
          aggregateId: id,
          payload: { id, content: createContent(id) },
        }))
        .sort(compare),
    );
  });

  test('Outbox message publishing is retried if it threw an error.', async () => {
    // Arrange
    const entityId = uuid();
    const content = createContent(entityId);
    let inboxMessageReceived: InboxMessage | undefined;
    const inboxMessageHandler = {
      aggregateType: aggregateType,
      eventType: eventType,
      handle: async (
        message: InboxMessage,
        _client: ClientBase,
      ): Promise<void> => {
        inboxMessageReceived = message;
      },
    };
    let threwOnce = false;
    const [storeOutboxMessage, shutdown] = await setupProducerAndConsumer(
      configs,
      [inboxMessageHandler],
      () => {
        if (!threwOnce) {
          threwOnce = true;
          throw new Error('First attempt failed');
        }
        return true;
      },
    );
    cleanup = shutdown;
    // wait a bit so older WAL messages are received
    await sleep(500);

    // Act
    await executeTransaction(loginPool, async (client: PoolClient) => {
      const entity = await client.query(
        `INSERT INTO public.source_entities (id, content) VALUES ($1, $2) RETURNING id, content;`,
        [entityId, content],
      );
      expect(entity.rowCount).toBe(1);
      await storeOutboxMessage(entityId, entity.rows[0], client);
    });

    // Assert
    const timeout = Date.now() + 5000; // 5 secs
    while (!inboxMessageReceived && Date.now() < timeout) {
      await sleep(100);
    }
    expect(inboxMessageReceived).toMatchObject({
      aggregateType,
      eventType,
      aggregateId: entityId,
      payload: { id: entityId, content },
    });
  });
});

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
  initializeOutboxMessageStorage,
  initializeOutboxService,
  logger,
  OutboxMessage,
  setLogger,
} from 'pg-transactional-outbox';
import { DockerComposeEnvironment } from 'testcontainers';
import { StartedDockerComposeEnvironment } from 'testcontainers/dist/docker-compose-environment/started-docker-compose-environment';
import {
  getConfigs,
  TestConfigs,
  setupTestDb,
  sleep,
  isDebugMode,
} from './test-utils';

if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(60_000);
  // Hide logs if the tests are not run in debug mode
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
}
const aggregateType = 'source_entity';
const eventType = 'source_entity_created';
const setupProducerAndConsumer = async (
  { inboxServiceConfig, outboxServiceConfig }: TestConfigs,
  inboxMessageHandlers: InboxMessageHandler[],
  messagePublisherWrapper?: (message: OutboxMessage) => boolean,
): Promise<
  [
    storeOutboxMessage: ReturnType<typeof initializeOutboxMessageStorage>,
    shutdown: () => Promise<void>,
  ]
> => {
  // Inbox
  const [inSrvShutdown] = await initializeInboxService(
    inboxServiceConfig,
    inboxMessageHandlers,
  );
  const [storeInboxMessage, inStoreShutdown] =
    await initializeInboxMessageStorage(inboxServiceConfig);

  // A simple in-process message sender
  const messageReceiver = async (message: OutboxMessage): Promise<void> => {
    await storeInboxMessage(message);
  };
  const messagePublisher = async (message: OutboxMessage): Promise<void> => {
    if (
      messagePublisherWrapper === undefined ||
      messagePublisherWrapper(message)
    ) {
      await messageReceiver(message);
    }
  };

  // Outbox
  const [outSrvShutdown] = await initializeOutboxService(
    outboxServiceConfig,
    messagePublisher,
  );
  const storeOutboxMessage = initializeOutboxMessageStorage(
    aggregateType,
    eventType,
    outboxServiceConfig,
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

const insertSourceEntity = async (
  loginPool: Pool,
  id: string,
  content: string,
  storeOutboxMessage: ReturnType<typeof initializeOutboxMessageStorage>,
) => {
  await executeTransaction(loginPool, async (client: PoolClient) => {
    const entity = await client.query(
      `INSERT INTO public.source_entities (id, content) VALUES ($1, $2) RETURNING id, content;`,
      [id, content],
    );
    if (entity.rowCount !== 1) {
      throw new Error(
        `Inserted ${entity.rowCount} source entities instead of 1.`,
      );
    }
    await storeOutboxMessage(id, entity.rows[0], client);
  });
};

const compareEntities = (
  { aggregateId: id1 }: { aggregateId: string },
  { aggregateId: id2 }: { aggregateId: string },
) => (id1 > id2 ? 1 : id2 > id1 ? -1 : 0);

describe('Sending messages from a producer to a consumer works.', () => {
  let dockerEnv: DockerComposeEnvironment;
  let startedEnv: StartedDockerComposeEnvironment;
  let loginPool: Pool;
  let configs: TestConfigs;
  let cleanup: { (): Promise<void> } | undefined = undefined;

  beforeAll(async () => {
    dockerEnv = new DockerComposeEnvironment(
      resolve(__dirname, 'test-utils'),
      'docker-compose.yml',
    );
    startedEnv = await dockerEnv.up();

    const postgresContainer = startedEnv.getContainer('postgres');
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
      cleanup().catch((e) => logger().error(e));
    }
  });

  afterAll(async () => {
    loginPool?.end().catch((e) => logger().error(e));
    startedEnv?.down().catch((e) => logger().error(e));
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
    await insertSourceEntity(loginPool, entityId, content, storeOutboxMessage);

    // Assert
    const timeout = Date.now() + 5_000;
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
    const timeout = Date.now() + 5_000;
    while (receivedInboxMessages.length < 10 && Date.now() < timeout) {
      await sleep(100);
    }
    expect(receivedInboxMessages).toHaveLength(10);
    expect(receivedInboxMessages.sort(compareEntities)).toMatchObject(
      ids
        .map((id) => ({
          aggregateType,
          eventType,
          aggregateId: id,
          payload: { id, content: createContent(id) },
        }))
        .sort(compareEntities),
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
    await insertSourceEntity(loginPool, entityId, content, storeOutboxMessage);

    // Assert
    const timeout = Date.now() + 5_000;
    while (!inboxMessageReceived && Date.now() < timeout) {
      await sleep(100);
    }
    expect(inboxMessageReceived).toBeDefined();
    expect(inboxMessageReceived).toMatchObject({
      aggregateType,
      eventType,
      aggregateId: entityId,
      payload: { id: entityId, content },
    });
  });

  test('100 messages are reliably sent even if some parts throw errors.', async () => {
    // Arrange
    const uuids = Array.from(Array(100), () => uuid());
    const inboxMessageReceived: InboxMessage[] = [];
    const inboxMessageHandler = {
      aggregateType: aggregateType,
      eventType: eventType,
      handle: async (
        message: InboxMessage,
        _client: ClientBase,
      ): Promise<void> => {
        inboxMessageReceived.push(message);
      },
    };
    let maxErrors = 10;
    const [storeOutboxMessage, shutdown] = await setupProducerAndConsumer(
      configs,
      [inboxMessageHandler],
      () => {
        if (Math.random() < 0.1 && maxErrors-- > 0) {
          throw new Error('Some fake error in the message sending process.');
        }
        return true;
      },
    );
    cleanup = shutdown;
    // wait a bit so older WAL messages are received
    await sleep(500);

    // Act
    await Promise.all(
      uuids.map(async (msgId) =>
        insertSourceEntity(
          loginPool,
          msgId,
          JSON.stringify({ id: uuid(), content: 'movie' }),
          storeOutboxMessage,
        ),
      ),
    );

    // Assert
    const timeout = Date.now() + 60_000;
    while (inboxMessageReceived.length !== 100 && Date.now() < timeout) {
      await sleep(100);
    }
    expect(inboxMessageReceived).toHaveLength(100);
    expect(inboxMessageReceived.sort(compareEntities)).toMatchObject(
      uuids.map((id) => ({ aggregateId: id })).sort(compareEntities),
    );
  });

  test('A single message is sent and received even with two outbox services', async () => {
    // Arrange
    const entityId = uuid();
    const content = createContent(entityId);
    let receivedFromOutbox1: OutboxMessage | null = null;
    let receivedFromOutbox2: OutboxMessage | null = null;
    const storeOutboxMessage = initializeOutboxMessageStorage(
      aggregateType,
      eventType,
      configs.outboxServiceConfig,
    );
    const [shutdown1] = await initializeOutboxService(
      configs.outboxServiceConfig,
      async (msg) => {
        receivedFromOutbox1 = msg;
      },
    );
    await sleep(500); // enough time for the first one to start up
    const [shutdown2] = await initializeOutboxService(
      configs.outboxServiceConfig,
      async (msg) => {
        receivedFromOutbox2 = msg;
      },
    );
    cleanup = async () => {
      await Promise.all([shutdown1(), shutdown2()]);
    };

    // Act
    await insertSourceEntity(loginPool, entityId, content, storeOutboxMessage);

    // Assert
    await sleep(500);
    expect(receivedFromOutbox1).not.toBeNull();
    // The second service does not start as only one reader per slot is allowed
    expect(receivedFromOutbox2).toBeNull();
  });

  test('An inbox message is acknowledged if there is no handler for it.', async () => {
    // Arrange
    const msg1: OutboxMessage = {
      id: uuid(),
      aggregateId: uuid(),
      aggregateType,
      eventType,
      payload: { content: 'some movie' },
      createdAt: '2023-01-18T21:02:27.000Z',
    };
    const msg2: OutboxMessage = {
      ...msg1,
      id: uuid(),
      aggregateId: uuid(),
      eventType: 'something_else',
    };
    const processedMessages: InboxMessage[] = [];
    const [storeInboxMessage, shutdownInboxStorage] =
      await initializeInboxMessageStorage(configs.inboxServiceConfig);
    const [shutdownInboxSrv] = await initializeInboxService(
      configs.inboxServiceConfig,
      [
        {
          aggregateType,
          eventType,
          handle: async (message: InboxMessage): Promise<void> => {
            processedMessages.push(message);
          },
        },
      ],
    );
    cleanup = async () => {
      await shutdownInboxStorage();
      await shutdownInboxSrv();
    };

    // Act
    await storeInboxMessage(msg1);
    await storeInboxMessage(msg2);

    // Assert
    await sleep(500);
    expect(processedMessages).toHaveLength(1);
    expect(processedMessages[0]).toMatchObject(msg1);

    // Check that the second message is not received now either
    await shutdownInboxSrv();
    let receivedMsg2: InboxMessage | null = null;
    const [shutdownInboxSrv2] = await initializeInboxService(
      configs.inboxServiceConfig,
      [
        {
          aggregateType,
          eventType: 'something_else',
          handle: async (message: InboxMessage): Promise<void> => {
            receivedMsg2 = message;
          },
        },
      ],
    );
    cleanup = async () => {
      await shutdownInboxStorage();
      await shutdownInboxSrv2();
    };
    await sleep(500);
    expect(receivedMsg2).toBeNull();
  });
});

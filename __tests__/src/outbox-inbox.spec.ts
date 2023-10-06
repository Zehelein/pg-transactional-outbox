/* eslint-disable @typescript-eslint/no-empty-function */
import { resolve } from 'path';
import { ClientBase, Pool, PoolClient } from 'pg';
import { v4 as uuid } from 'uuid';
import {
  disableLogger,
  executeTransaction,
  InboxMessage,
  InboxMessageHandler,
  initializeInboxMessageStorage,
  initializeInboxService,
  initializeOutboxMessageStorage,
  initializeOutboxService,
  logger,
  OutboxMessage,
} from 'pg-transactional-outbox';
import {
  DockerComposeEnvironment,
  StartedDockerComposeEnvironment,
} from 'testcontainers';
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
  disableLogger(); // Hide logs if the tests are not run in debug mode
}
const aggregateType = 'source_entity';
const messageType = 'source_entity_created';
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
  const [outSrvShutdown] = initializeOutboxService(
    outboxServiceConfig,
    messagePublisher,
  );
  const storeOutboxMessage = initializeOutboxMessageStorage(
    aggregateType,
    messageType,
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

describe('Outbox and inbox integration tests', () => {
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
      aggregateType,
      messageType,
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
      messageType,
      aggregateId: entityId,
      payload: { id: entityId, content },
    });
  });

  test('Ten messages are sent and received in the same sort order', async () => {
    // Arrange
    const receivedInboxMessages: InboxMessage[] = [];
    const inboxMessageHandler = {
      aggregateType,
      messageType,
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
    expect(receivedInboxMessages).toMatchObject(
      ids.map((id) => ({
        aggregateType,
        messageType,
        aggregateId: id,
        payload: { id, content: createContent(id) },
      })),
    );
  });

  test('Outbox message publishing is retried if it threw an error.', async () => {
    // Arrange
    const entityId = uuid();
    const content = createContent(entityId);
    let inboxMessageReceived: InboxMessage | undefined;
    const inboxMessageHandler = {
      aggregateType,
      messageType,
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
      messageType,
      aggregateId: entityId,
      payload: { id: entityId, content },
    });
  });

  test('100 messages are reliably sent and received in the same order even if some parts throw errors.', async () => {
    // Arrange
    const uuids = Array.from(Array(100), () => uuid());
    const inboxMessageReceived: InboxMessage[] = [];
    const inboxMessageHandler = {
      aggregateType,
      messageType,
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
      (m: OutboxMessage) => {
        if (Math.random() < 0.1 && maxErrors-- > 0) {
          throw new Error(
            `Some fake error when processing message with id ${m.id}.`,
          );
        }
        return true;
      },
    );
    cleanup = shutdown;
    // wait a bit so older WAL messages are received
    await sleep(500);

    // Act
    for (const msgId of uuids) {
      // Sequentially insert the messages to test message processing sort order
      await insertSourceEntity(
        loginPool,
        msgId,
        JSON.stringify({ id: uuid(), content: 'movie' }),
        storeOutboxMessage,
      );
    }

    // Assert
    const timeout = Date.now() + 30_000;
    let lastLength = 0;
    while (inboxMessageReceived.length !== 100 && Date.now() < timeout) {
      if (inboxMessageReceived.length > lastLength) {
        lastLength = inboxMessageReceived.length;
      }
      await sleep(100);
    }
    expect(timeout).toBeGreaterThanOrEqual(Date.now());
    expect(inboxMessageReceived).toHaveLength(100);
    expect(inboxMessageReceived.map((msg) => msg.aggregateId)).toStrictEqual(
      uuids,
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
      messageType,
      configs.outboxServiceConfig,
    );
    const [shutdown1] = initializeOutboxService(
      configs.outboxServiceConfig,
      async (msg) => {
        receivedFromOutbox1 = msg;
      },
    );
    await sleep(500); // enough time for the first one to start up
    const [shutdown2] = initializeOutboxService(
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
      messageType,
      payload: { content: 'some movie' },
      createdAt: '2023-01-18T21:02:27.000Z',
    };
    const msg2: OutboxMessage = {
      ...msg1,
      id: uuid(),
      aggregateId: uuid(),
      messageType: 'something_else',
    };
    const processedMessages: InboxMessage[] = [];
    const [storeInboxMessage, shutdownInboxStorage] =
      await initializeInboxMessageStorage(configs.inboxServiceConfig);
    const [shutdownInboxSrv] = await initializeInboxService(
      configs.inboxServiceConfig,
      [
        {
          aggregateType,
          messageType,
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
          messageType: 'something_else',
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

  test('An inbox message is is retried up to 5 times if it throws an error.', async () => {
    // Arrange
    const msg: OutboxMessage = {
      id: uuid(),
      aggregateId: uuid(),
      aggregateType,
      messageType,
      payload: { content: 'some movie' },
      createdAt: '2023-01-18T21:02:27.000Z',
    };
    const [storeInboxMessage, shutdownInboxStorage] =
      await initializeInboxMessageStorage(configs.inboxServiceConfig);
    let inboxHandlerCounter = 0;
    const [shutdownInboxSrv] = await initializeInboxService(
      configs.inboxServiceConfig,
      [
        {
          aggregateType,
          messageType,
          handle: async (): Promise<void> => {
            inboxHandlerCounter++;
            throw Error('Handling the inbox message failed');
          },
        },
      ],
    );
    cleanup = async () => {
      await shutdownInboxStorage();
      await shutdownInboxSrv();
    };

    // Act
    await storeInboxMessage(msg);

    // Assert
    await sleep(1000);
    expect(inboxHandlerCounter).toBe(5);
    const inboxResult = await loginPool.query(
      `SELECT retries FROM ${configs.inboxServiceConfig.settings.dbSchema}.${configs.inboxServiceConfig.settings.dbTable} WHERE id = $1;`,
      [msg.id],
    );
    expect(inboxResult.rowCount).toBe(1);
    expect(inboxResult.rows[0].retries).toBe(5);
  });

  test('Two messages are processed in order even if the first takes longer.', async () => {
    // Arrange
    const uuid1 = uuid();
    const uuid2 = uuid();
    const inboxMessageReceived: InboxMessage[] = [];
    const inboxMessageHandler = {
      aggregateType,
      messageType,
      handle: async (
        message: InboxMessage,
        _client: ClientBase,
      ): Promise<void> => {
        if (message.aggregateId === uuid1) {
          await sleep(250);
        }
        inboxMessageReceived.push(message);
      },
    };
    const [storeOutboxMessage, shutdown] = await setupProducerAndConsumer(
      configs,
      [inboxMessageHandler],
    );
    cleanup = shutdown;
    await sleep(1);

    // Act
    await insertSourceEntity(
      loginPool,
      uuid1,
      JSON.stringify({ id: uuid1, content: 'movie' }),
      storeOutboxMessage,
    );
    await insertSourceEntity(
      loginPool,
      uuid2,
      JSON.stringify({ id: uuid2, content: 'movie' }),
      storeOutboxMessage,
    );

    // Assert
    await sleep(200);
    expect(inboxMessageReceived).toHaveLength(0);
    await sleep(200);
    expect(inboxMessageReceived).toHaveLength(2);
    expect(inboxMessageReceived[0].aggregateId).toBe(uuid1);
    expect(inboxMessageReceived[1].aggregateId).toBe(uuid2);
  });
});

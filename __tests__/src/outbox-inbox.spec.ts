/* eslint-disable @typescript-eslint/no-empty-function */
import { resolve } from 'path';
import { Client, ClientBase, Pool, PoolClient } from 'pg';
import {
  InMemoryLogEntry,
  InboxMessage,
  InboxMessageHandler,
  OutboxMessage,
  TransactionalLogger,
  TransactionalOutboxInboxConfig,
  createFullConcurrencyController,
  createMutexConcurrencyController,
  executeTransaction,
  getInMemoryLogger,
  initializeInboxListener,
  initializeInboxMessageStorage,
  initializeOutboxListener,
  initializeOutboxMessageStorage,
} from 'pg-transactional-outbox';
import {
  DockerComposeEnvironment,
  StartedDockerComposeEnvironment,
} from 'testcontainers';
import { v4 as uuid } from 'uuid';
import {
  TestConfigs,
  getConfigs,
  isDebugMode,
  setupTestDb,
  sleep,
  sleepUntilTrue,
} from './test-utils';

if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(60_000);
}
const aggregateType = 'source_entity';
const messageType = 'source_entity_created';
const metadata = { routingKey: 'test.route', exchange: 'test-exchange' };
const setupProducerAndConsumer = (
  { inboxConfig, outboxConfig }: TestConfigs,
  inboxMessageHandlers: InboxMessageHandler[],
  inboxLogger: TransactionalLogger,
  outboxLogger: TransactionalLogger,
  messagePublisherWrapper?: (message: OutboxMessage) => boolean,
): [
  storeOutboxMessage: ReturnType<typeof initializeOutboxMessageStorage>,
  shutdown: () => Promise<void>,
] => {
  // Inbox
  const [inSrvShutdown] = initializeInboxListener(
    inboxConfig,
    inboxMessageHandlers,
    inboxLogger,
    createMutexConcurrencyController(),
  );
  const [storeInboxMessage, inStoreShutdown] = initializeInboxMessageStorage(
    inboxConfig,
    inboxLogger,
  );

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
  const [outSrvShutdown] = initializeOutboxListener(
    outboxConfig,
    messagePublisher,
    outboxLogger,
    createMutexConcurrencyController(),
  );
  const storeOutboxMessage = initializeOutboxMessageStorage(
    aggregateType,
    messageType,
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
    await storeOutboxMessage(id, entity.rows[0], client, metadata);
  });
};

describe('Outbox and inbox integration tests', () => {
  let dockerEnv: DockerComposeEnvironment;
  let startedEnv: StartedDockerComposeEnvironment;
  let loginPool: Pool;
  let configs: TestConfigs;
  let cleanup: { (): Promise<void> } | undefined = undefined;
  const [outboxLogger, outboxLogs] = getInMemoryLogger('outbox');
  const [inboxLogger, inboxLogs] = getInMemoryLogger('inbox');

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
      outboxLogger.error(err, 'PostgreSQL pool error');
      inboxLogger.error(err, 'PostgreSQL pool error');
    });
  });

  beforeEach(async () => {
    inboxLogs.length = 0;
    outboxLogs.length = 0;
    const { host, port } = configs.loginConnection;
    const resetReplication = async ({
      settings: { postgresSlot },
      pgReplicationConfig: { database },
    }: TransactionalOutboxInboxConfig) => {
      const rootInboxClient = new Client({
        host,
        port,
        user: 'postgres',
        password: 'postgres',
        database,
      });
      try {
        rootInboxClient.connect();
        await rootInboxClient.query(
          `SELECT * FROM pg_replication_slot_advance('${postgresSlot}', pg_current_wal_lsn());`,
        );
      } finally {
        rootInboxClient.end();
      }
    };
    await resetReplication(configs.inboxConfig);
    await resetReplication(configs.outboxConfig);
  });

  afterEach(async () => {
    if (cleanup) {
      try {
        await cleanup();
      } catch (e) {
        inboxLogger.error(e);
        outboxLogger.error(e);
      }
    }
  });

  afterAll(async () => {
    try {
      await loginPool?.end();
      await startedEnv?.down();
    } catch (e) {
      inboxLogger.error(e);
      outboxLogger.error(e);
    }
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
    const [storeOutboxMessage, shutdown] = setupProducerAndConsumer(
      configs,
      [inboxMessageHandler],
      inboxLogger,
      outboxLogger,
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
      metadata,
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
    const [storeOutboxMessage, shutdown] = setupProducerAndConsumer(
      configs,
      [inboxMessageHandler],
      inboxLogger,
      outboxLogger,
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
          await storeOutboxMessage(id, entity.rows[0], client, metadata);
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
        metadata,
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
    const [storeOutboxMessage, shutdown] = setupProducerAndConsumer(
      configs,
      [inboxMessageHandler],
      inboxLogger,
      outboxLogger,
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
      metadata,
    });
  });

  test('100 messages are reliably sent and received in the same order even if some parts throw errors.', async () => {
    // Arrange
    const uuids = Array.from(Array(100), () => uuid());
    const inboxMessageReceived: InboxMessage[] = [];
    let maxErrors = 10;
    const inboxMessageHandler = {
      aggregateType,
      messageType,
      handle: async (
        message: InboxMessage,
        _client: ClientBase,
      ): Promise<void> => {
        if (Math.random() < 0.1 && maxErrors-- > 0) {
          inboxLogger.fatal(
            `Throwing now an error for message ID ${message.id}`,
          );
          throw new Error(
            `Some fake error when processing message with id ${message.id}.`,
          );
        }
        inboxMessageReceived.push(message);
      },
    };
    const [storeOutboxMessage, shutdown] = setupProducerAndConsumer(
      configs,
      [inboxMessageHandler],
      inboxLogger,
      outboxLogger,
    );
    cleanup = shutdown;
    // wait a bit so older WAL messages are received
    await sleep(500);

    // Act
    for (const id of uuids) {
      // Sequentially insert the messages to test message processing sort order
      await insertSourceEntity(
        loginPool,
        id,
        JSON.stringify({ id, content: 'movie' }),
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
      await sleep(10);
    }
    expect(timeout).toBeGreaterThanOrEqual(Date.now());
    expect(inboxMessageReceived).toHaveLength(100);
    expect(inboxMessageReceived.map((msg) => msg.aggregateId)).toStrictEqual(
      uuids,
    );
  });

  test('A single message is sent and received even with two outbox listeners', async () => {
    // Arrange
    const entityId = uuid();
    const content = createContent(entityId);
    let receivedFromOutbox1: OutboxMessage | null = null;
    let receivedFromOutbox2: OutboxMessage | null = null;
    const storeOutboxMessage = initializeOutboxMessageStorage(
      aggregateType,
      messageType,
      configs.outboxConfig,
    );
    const [shutdown1] = initializeOutboxListener(
      configs.outboxConfig,
      async (msg) => {
        receivedFromOutbox1 = msg;
      },
      outboxLogger,
      createMutexConcurrencyController(),
    );
    await sleep(500); // enough time for the first one to start up
    const [shutdown2] = initializeOutboxListener(
      configs.outboxConfig,
      async (msg) => {
        receivedFromOutbox2 = msg;
      },
      outboxLogger,
      createMutexConcurrencyController(),
    );

    // Act
    await insertSourceEntity(loginPool, entityId, content, storeOutboxMessage);

    // Assert
    await sleep(500);
    expect(receivedFromOutbox1).not.toBeNull();
    // The second listener does not start as only one reader per slot is allowed
    expect(receivedFromOutbox2).toBeNull();

    cleanup = async () => {
      await shutdown1();
      await shutdown2();
    };
  });

  test('An inbox message is acknowledged if there is no handler for it.', async () => {
    // Arrange
    const msg1: OutboxMessage = {
      id: uuid(),
      aggregateId: uuid(),
      aggregateType,
      messageType,
      payload: { content: 'some movie' },
      metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
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
      initializeInboxMessageStorage(configs.inboxConfig, inboxLogger);
    const [shutdownInboxSrv] = initializeInboxListener(
      configs.inboxConfig,
      [
        {
          aggregateType,
          messageType,
          handle: async (message: InboxMessage): Promise<void> => {
            processedMessages.push(message);
          },
        },
      ],
      inboxLogger,
      createMutexConcurrencyController(),
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
    const [shutdownInboxSrv2] = initializeInboxListener(
      configs.inboxConfig,
      [
        {
          aggregateType,
          messageType: 'something_else',
          handle: async (message: InboxMessage): Promise<void> => {
            receivedMsg2 = message;
          },
        },
      ],
      inboxLogger,
      createMutexConcurrencyController(),
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
      metadata,
      createdAt: '2023-01-18T21:02:27.000Z',
    };
    const [storeInboxMessage, shutdownInboxStorage] =
      initializeInboxMessageStorage(configs.inboxConfig, inboxLogger);
    let inboxHandlerCounter = 0;
    const [shutdownInboxSrv] = initializeInboxListener(
      configs.inboxConfig,
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
      inboxLogger,
      createMutexConcurrencyController(),
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
      `SELECT attempts FROM ${configs.inboxConfig.settings.dbSchema}.${configs.inboxConfig.settings.dbTable} WHERE id = $1;`,
      [msg.id],
    );
    expect(inboxResult.rowCount).toBe(1);
    expect(inboxResult.rows[0].attempts).toBe(5);
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
      inboxLogger,
      outboxLogger,
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
    await sleep(500); // depending
    expect(inboxMessageReceived).toHaveLength(2);
    expect(inboxMessageReceived[0].aggregateId).toBe(uuid1);
    expect(inboxMessageReceived[1].aggregateId).toBe(uuid2);
  });

  test('With concurrent processing and a slow first and fast second message the acknowledgement is only sent after the first is also done.', async () => {
    // Arrange
    const createMsg: (id: number) => OutboxMessage = (id: number) => ({
      id: uuid(),
      aggregateId: id.toString(),
      aggregateType,
      messageType,
      payload: { content: `movie ${id}` },
      metadata,
      createdAt: '2023-01-18T21:02:27.000Z',
    });
    const msg1 = createMsg(1);
    const msg2 = createMsg(2);

    // Act
    const [storeInboxMessage, shutdownInboxStorage] =
      initializeInboxMessageStorage(configs.inboxConfig, inboxLogger);

    await storeInboxMessage(msg1);
    await storeInboxMessage(msg2);

    const items: string[] = [];
    const [shutdownInboxSrv] = initializeInboxListener(
      configs.inboxConfig,
      [
        {
          aggregateType,
          messageType,
          handle: async (message): Promise<void> => {
            if (message.id === msg1.id) {
              await sleep(250);
            }
            await sleep(20);
            items.push(message.id);
          },
        },
      ],
      inboxLogger,
      createFullConcurrencyController(),
    );
    cleanup = async () => {
      await shutdownInboxStorage();
      await shutdownInboxSrv();
    };

    // Assert - verify that the first message was not handled for now but the second one is
    expect(items).toHaveLength(0);
    const time1 = await sleepUntilTrue(() => items.length > 0, 500);
    expect(items.length).toBeGreaterThanOrEqual(1);
    expect(time1).toBeLessThanOrEqual(250);
    expect(items[0]).toBe(msg2.id);
    await sleepUntilTrue(() => items.length === 2, 500);
    expect(items[1]).toBe(msg1.id);
    const findRegex = (msg: OutboxMessage) =>
      new RegExp(
        `Finished processing LSN .* with message id ${msg.id} and type ${msg.messageType}.`,
        'g',
      );
    const findIndex = (regex: RegExp) =>
      inboxLogs.findIndex(
        (log: InMemoryLogEntry) =>
          log.args.length === 2 &&
          typeof log.args[1] === 'string' &&
          log.args[1].match(regex),
      );
    const msg1Index = findIndex(findRegex(msg1));
    const msg2Index = findIndex(findRegex(msg2));
    expect(msg1Index).toBe(-1); // The message 1 acknowledgement is skipped as it was done as part of the message 2 acknowledgement
    expect(msg2Index).toBeGreaterThan(0);
  });
});

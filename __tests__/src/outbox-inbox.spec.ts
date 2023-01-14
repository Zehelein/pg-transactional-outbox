/* eslint-disable @typescript-eslint/no-empty-function */
import { resolve } from 'path';
import { Client, ClientBase, Pool, PoolClient } from 'pg';
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
import {
  getConfigs,
  TestConfigs,
  setupTestDb,
  sleep,
  tryCallback,
} from './test-utils';

jest.setTimeout(60_000);
// Uncomment to not see logs during tests:
// const fakePinoLogger = {
//   child: () => fakePinoLogger,
//   debug: () => {},
//   error: () => {},
//   fatal: () => {},
//   info: () => {},
//   trace: () => {},
//   warn: () => {},
//   silent: () => {},
//   level: 'silent',
// };
//setLogger(fakePinoLogger);

const aggregateType = 'source_entity';
const eventType = 'source_entity_created';
const setupProducerAndConsumer = async (
  { inboxServiceConfig, outboxServiceConfig }: TestConfigs,
  inboxMessageHandlers: InboxMessageHandler[],
  outboxPublisherWrapper?: (message: OutboxMessage) => boolean,
): Promise<
  [
    storeOutboxMessage: ReturnType<typeof initializeOutboxMessageStore>,
    shutdown: () => Promise<void>,
  ]
> => {
  // Inbox
  const { shutdown: inSrvShutdown } = await initializeInboxService(
    inboxServiceConfig,
    inboxMessageHandlers,
  );
  const { storeInboxMessage, shutdown: inStoreShutdown } =
    await initializeInboxMessageStorage(inboxServiceConfig);

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
  storeOutboxMessage: ReturnType<typeof initializeOutboxMessageStore>,
) => {
  await executeTransaction(loginPool, async (client: PoolClient) => {
    const entity = await client.query(
      `INSERT INTO public.source_entities (id, content) VALUES ($1, $2) RETURNING id, content;`,
      [id, content],
    );
    expect(entity.rowCount).toBe(1);
    await storeOutboxMessage(id, entity.rows[0], client);
  });
};

const compareEntities = (
  { aggregateId: id1 }: { aggregateId: string },
  { aggregateId: id2 }: { aggregateId: string },
) => (id1 > id2 ? 1 : id2 > id1 ? -1 : 0);

const createInfraOutage = async (
  startedEnv: StartedDockerComposeEnvironment,
) => {
  // Stop the environment and 5 seconds later start the PG container again
  await startedEnv.stop();
  setTimeout(async () => {
    const postgresContainer = startedEnv.getContainer('postgres');
    await postgresContainer.restart();
  }, 5000);
};

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
    loginPool.end().catch((e) => logger().error(e));
    startedEnv.down().catch((e) => logger().error(e));
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
    const timeout = Date.now() + 50000000; // 5 secs
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
    const timeout = Date.now() + 5000000; // 5 secs
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

  test('Ensure reconnection possible after PostgreSQL outage.', async () => {
    const ensureDbConnection = async () => {
      let client: Client | undefined = undefined;
      try {
        client = new Client(configs.loginConnection);
        await client.connect();
        const one = await client.query(`SELECT 1 as one`);
        expect(one).toMatchObject({
          rowCount: 1,
          rows: [{ one: 1 }],
        });
      } finally {
        await client?.end();
      }
    };

    await ensureDbConnection();
    await createInfraOutage(startedEnv);
    await tryCallback(ensureDbConnection, 10_000, 100);
  });
});

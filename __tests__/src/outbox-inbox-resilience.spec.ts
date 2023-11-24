/* eslint-disable @typescript-eslint/no-empty-function */
import { resolve } from 'path';
import { Client, ClientBase, Pool, PoolClient } from 'pg';
import {
  InboxMessage,
  OutboxMessage,
  TransactionalLogger,
  createMutexConcurrencyController,
  executeTransaction,
  getDefaultLogger,
  getDisabledLogger,
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
  retryCallback,
  setupTestDb,
} from './test-utils';

if (isDebugMode()) {
  jest.setTimeout(600_000);
} else {
  jest.setTimeout(90_000);
}
const aggregateType = 'source_entity';
const messageType = 'source_entity_created';
const metadata = { routingKey: 'test.route', exchange: 'test-exchange' };

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

const compareEntities = (
  { aggregateId: id1 }: { aggregateId: string },
  { aggregateId: id2 }: { aggregateId: string },
) => (id1 > id2 ? 1 : id2 > id1 ? -1 : 0);

const createInfraOutage = async (
  startedEnv: StartedDockerComposeEnvironment,
  logger: TransactionalLogger,
) => {
  try {
    // Stop the environment and a bit later start the PG container again
    await startedEnv.stop();
  } catch (error) {
    logger.error(error);
  }
  setTimeout(async () => {
    try {
      const postgresContainer = startedEnv.getContainer('postgres-resilience');
      await postgresContainer.restart();
    } catch (error) {
      logger.error(error);
    }
  }, 3000);
};

describe('Outbox and inbox resilience integration tests', () => {
  let dockerEnv: DockerComposeEnvironment;
  let startedEnv: StartedDockerComposeEnvironment;
  let loginPool: Pool;
  let configs: TestConfigs;
  let cleanup: { (): Promise<void> } | undefined = undefined;
  const logger = isDebugMode() ? getDefaultLogger() : getDisabledLogger();

  beforeAll(async () => {
    dockerEnv = new DockerComposeEnvironment(
      resolve(__dirname, 'test-utils'),
      'docker-compose-resilience.yml',
    );
    startedEnv = await dockerEnv.up();

    configs = getConfigs(60399);
    await setupTestDb(configs);

    loginPool = new Pool(configs.loginConnection);
    loginPool.on('error', (err) => {
      logger.error(err, 'PostgreSQL pool error');
    });
  });

  afterEach(async () => {
    if (cleanup) {
      try {
        await cleanup();
      } catch (e) {
        logger.error(e);
      }
    }
  });

  afterAll(async () => {
    try {
      await loginPool?.end();
      await startedEnv?.down();
    } catch (e) {
      logger.error(e);
    }
  });

  test('Messages are stored and later sent even if the PostgreSQL server goes down', async () => {
    // Arrange
    const entity1Id = uuid();
    const content1 = createContent(entity1Id);
    const entity2Id = uuid();
    const content2 = createContent(entity2Id);
    const sentMessages: OutboxMessage[] = [];
    const storeOutboxMessage = initializeOutboxMessageStorage(
      aggregateType,
      messageType,
      configs.outboxConfig,
    );

    // Act
    // Store two message before starting up the outbox listener
    await insertSourceEntity(
      loginPool,
      entity1Id,
      content1,
      storeOutboxMessage,
    );
    await insertSourceEntity(
      loginPool,
      entity2Id,
      content2,
      storeOutboxMessage,
    );
    // Stop the PostgreSQL docker container and restart it after a few seconds while
    // the outbox listener starts. The outbox listener will retry for a while
    await createInfraOutage(startedEnv, logger);
    // Start the listener - it should succeed after PG is up again
    const [shutdown] = initializeOutboxListener(
      configs.outboxConfig,
      async (msg) => {
        sentMessages.push(msg);
      },
      logger,
      createMutexConcurrencyController(),
    );
    cleanup = shutdown;

    // Assert
    await retryCallback(
      async () => {
        if (sentMessages.length !== 2) {
          throw new Error('Messages did not arrive - retry again');
        }
      },
      60_000,
      100,
    );
    expect(sentMessages).toHaveLength(2);
    expect(sentMessages.sort(compareEntities)).toMatchObject(
      [
        {
          aggregateType,
          messageType,
          aggregateId: entity1Id,
          payload: { id: entity1Id, content: content1 },
          metadata,
        },
        {
          aggregateType,
          messageType,
          aggregateId: entity2Id,
          payload: { id: entity2Id, content: content2 },
          metadata,
        },
      ].sort(compareEntities),
    );
  });

  test('Messages are stored in the inbox and fully delivered even if the PostgreSQL server goes down', async () => {
    // Arrange
    const msg1: OutboxMessage = {
      id: uuid(),
      aggregateId: uuid(),
      aggregateType,
      messageType,
      payload: { content: 'some movie' },
      metadata,
      createdAt: '2023-01-18T21:02:27.000Z',
    };
    const msg2: OutboxMessage = {
      ...msg1,
      id: uuid(),
      aggregateId: uuid(),
    };
    const processedMessages: InboxMessage[] = [];
    const [storeInboxMessage, shutdownInboxStorage] =
      initializeInboxMessageStorage(configs.inboxConfig, logger);

    // Act
    // Store two message before starting up the inbox listener
    await storeInboxMessage(msg1);
    await storeInboxMessage(msg2);
    // Stop the PostgreSQL docker container and restart it after a few seconds while
    // the inbox listener starts. The inbox listener will retry for a while
    await createInfraOutage(startedEnv, logger);
    // Start the listener - it should succeed after PG is up again
    const [shutdownInboxSrv] = initializeInboxListener(
      configs.inboxConfig,
      [
        {
          aggregateType,
          messageType,
          handle: async (
            message: InboxMessage,
            client: ClientBase,
          ): Promise<void> => {
            await client.query('SELECT NOW() as now');
            processedMessages.push(message);
          },
        },
      ],
      logger,
      createMutexConcurrencyController(),
    );
    cleanup = async () => {
      await shutdownInboxStorage();
      await shutdownInboxSrv();
    };

    // Assert
    await retryCallback(
      async () => {
        if (processedMessages.length !== 2) {
          throw new Error('Messages did not arrive - retry again');
        }
      },
      60_000,
      100,
    );
    expect(processedMessages).toHaveLength(2);
    expect(processedMessages.sort(compareEntities)).toMatchObject(
      [msg1, msg2].sort(compareEntities),
    );
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
    await createInfraOutage(startedEnv, logger);
    await retryCallback(ensureDbConnection, 10_000, 100);
  });
});

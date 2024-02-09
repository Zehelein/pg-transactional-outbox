/* eslint-disable @typescript-eslint/no-empty-function */
import { resolve } from 'path';
import { Client, Pool, PoolClient } from 'pg';
import {
  StoredTransactionalMessage,
  TransactionalLogger,
  TransactionalMessage,
  executeTransaction,
  getDefaultLogger,
  getDisabledLogger,
  initializeMessageStorage,
  initializeReplicationMessageListener,
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
  setupReplicationTestDb,
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
  client: PoolClient,
  id: string,
  content: string,
  storeOutboxMessage: ReturnType<typeof initializeMessageStorage>,
) => {
  await executeTransaction(client, async (txnClient) => {
    const entity = await txnClient.query(
      `INSERT INTO public.source_entities (id, content) VALUES ($1, $2) RETURNING id, content;`,
      [id, content],
    );
    if (entity.rowCount !== 1) {
      throw new Error(
        `Inserted ${entity.rowCount} source entities instead of 1.`,
      );
    }
    const message: TransactionalMessage = {
      id: uuid(),
      aggregateId: id,
      aggregateType,
      messageType,
      createdAt: new Date().toISOString(),
      payload: entity.rows[0],
      metadata,
    };
    await storeOutboxMessage(message, txnClient);
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

describe('Outbox and inbox resilience integration tests for replication', () => {
  let dockerEnv: DockerComposeEnvironment;
  let startedEnv: StartedDockerComposeEnvironment;
  let handlerPool: Pool;
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
    await setupReplicationTestDb(configs);

    handlerPool = new Pool(configs.handlerConnection);
    handlerPool.on('error', (err) => {
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
      await handlerPool?.end();
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
    const sentMessages: TransactionalMessage[] = [];
    const storeOutboxMessage = initializeMessageStorage(
      configs.outboxConfig,
      logger,
    );

    // Act
    // Store two message before starting up the outbox listener
    await insertSourceEntity(
      await handlerPool.connect(),
      entity1Id,
      content1,
      storeOutboxMessage,
    );
    await insertSourceEntity(
      await handlerPool.connect(),
      entity2Id,
      content2,
      storeOutboxMessage,
    );
    // Stop the PostgreSQL docker container and restart it after a few seconds while
    // the outbox listener starts. The outbox listener will retry for a while
    await createInfraOutage(startedEnv, logger);
    // Start the listener - it should succeed after PG is up again
    const [shutdown] = initializeReplicationMessageListener(
      configs.outboxConfig,
      {
        handle: async (msg) => {
          sentMessages.push(msg);
        },
      },
      logger,
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
    const msg1: TransactionalMessage = {
      id: uuid(),
      aggregateId: uuid(),
      aggregateType,
      messageType,
      payload: { content: 'some movie' },
      metadata,
      createdAt: '2023-01-18T21:02:27.000Z',
    };
    const msg2: TransactionalMessage = {
      ...msg1,
      id: uuid(),
      aggregateId: uuid(),
    };
    const processedMessages: StoredTransactionalMessage[] = [];
    const storeInboxMessage = initializeMessageStorage(
      configs.inboxConfig,
      logger,
    );

    // Act
    // Store two message before starting up the inbox listener
    await executeTransaction(await handlerPool.connect(), async (client) => {
      await storeInboxMessage(msg1, client);
      await storeInboxMessage(msg2, client);
    });
    // Stop the PostgreSQL docker container and restart it after a few seconds while
    // the inbox listener starts. The inbox listener will retry for a while
    await createInfraOutage(startedEnv, logger);
    // Start the listener - it should succeed after PG is up again
    const [shutdownInboxSrv] = initializeReplicationMessageListener(
      configs.inboxConfig,
      [
        {
          aggregateType,
          messageType,
          handle: async (
            message: StoredTransactionalMessage,
            client: PoolClient,
          ): Promise<void> => {
            await client.query('SELECT NOW() as now');
            processedMessages.push(message);
          },
        },
      ],
      logger,
    );
    cleanup = async () => {
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
        client = new Client(configs.handlerConnection);
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

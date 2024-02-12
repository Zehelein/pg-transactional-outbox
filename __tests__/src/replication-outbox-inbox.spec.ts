/* eslint-disable @typescript-eslint/no-empty-function */
import { resolve } from 'path';
import { Client, Pool } from 'pg';
import {
  DatabaseClient,
  InMemoryLogEntry,
  ReplicationConfig,
  StoredTransactionalMessage,
  TransactionalLogger,
  TransactionalMessage,
  TransactionalMessageHandler,
  createReplicationDiscriminatingMutexConcurrencyController,
  createReplicationFullConcurrencyController,
  createReplicationMultiConcurrencyController,
  executeTransaction,
  getInMemoryLogger,
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
  setupReplicationTestDb,
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
  inboxMessageHandlers: TransactionalMessageHandler[],
  inboxLogger: TransactionalLogger,
  outboxLogger: TransactionalLogger,
  messagePublisherWrapper?: (message: TransactionalMessage) => boolean,
): [
  storeOutboxMessage: ReturnType<typeof initializeMessageStorage>,
  shutdown: () => Promise<void>,
] => {
  // Inbox
  const [inSrvShutdown] = initializeReplicationMessageListener(
    inboxConfig,
    inboxMessageHandlers,
    inboxLogger,
  );
  const storeInboxMessage = initializeMessageStorage(inboxConfig, inboxLogger);

  // A simple in-process message sender
  const pool = new Pool(inboxConfig.dbHandlerConfig);
  const messageReceiver = async (
    message: TransactionalMessage,
  ): Promise<void> => {
    await executeTransaction(await pool.connect(), async (client) => {
      await storeInboxMessage(message, client);
    });
  };
  const messagePublisher = {
    handle: async (message: TransactionalMessage): Promise<void> => {
      if (
        messagePublisherWrapper === undefined ||
        messagePublisherWrapper(message)
      ) {
        await messageReceiver(message);
      }
    },
  };

  // Outbox
  const [outSrvShutdown] = initializeReplicationMessageListener(
    outboxConfig,
    messagePublisher,
    outboxLogger,
  );
  const storeOutboxMessage = initializeMessageStorage(
    outboxConfig,
    outboxLogger,
  );

  return [
    storeOutboxMessage,
    async () => {
      await pool.end();
      await inSrvShutdown();
      await outSrvShutdown();
    },
  ];
};

const createContent = (id: string) => `Content for id ${id}`;

const createMsg = (
  overrides?: Partial<TransactionalMessage>,
): TransactionalMessage => ({
  id: uuid(),
  aggregateId: uuid(),
  aggregateType,
  messageType,
  payload: { content: 'some movie' },
  metadata,
  createdAt: '2023-01-18T21:02:27.000Z',
  ...overrides,
});

const insertSourceEntity = async (
  handlerPool: Pool,
  id: string,
  content: string,
  storeOutboxMessage: ReturnType<typeof initializeMessageStorage>,
) => {
  await executeTransaction(await handlerPool.connect(), async (client) => {
    const entity = await client.query(
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
      payload: entity.rows[0],
      metadata,
      createdAt: new Date().toISOString(),
    };
    await storeOutboxMessage(message, client);
  });
};

describe('Replication integration tests', () => {
  let dockerEnv: DockerComposeEnvironment;
  let startedEnv: StartedDockerComposeEnvironment;
  let handlerPool: Pool;
  let configs: TestConfigs;
  let cleanup: { (): Promise<void> } | undefined = undefined;
  const [outboxLogger, outboxLogs] = getInMemoryLogger('outbox');
  const [inboxLogger, inboxLogs] = getInMemoryLogger('inbox');

  beforeAll(async () => {
    dockerEnv = new DockerComposeEnvironment(
      resolve(__dirname, 'test-utils'),
      'docker-compose-replication.yml',
    );
    startedEnv = await dockerEnv.up();

    const postgresContainer = startedEnv.getContainer('postgres-replication');
    const port = postgresContainer.getMappedPort(5432);
    configs = getConfigs(port);
    await setupReplicationTestDb(configs);

    handlerPool = new Pool(configs.handlerConnection);
    handlerPool.on('error', (err) => {
      outboxLogger.error(err, 'PostgreSQL pool error');
      inboxLogger.error(err, 'PostgreSQL pool error');
    });
  });

  beforeEach(async () => {
    inboxLogs.length = 0;
    outboxLogs.length = 0;
    const { host, port } = configs.handlerConnection;
    const resetReplication = async ({
      settings: { postgresSlot },
      dbListenerConfig: { database },
    }: ReplicationConfig) => {
      const rootInboxClient = new Client({
        host,
        port,
        user: 'postgres',
        password: 'postgres',
        database,
      });
      try {
        await rootInboxClient.connect();
        await rootInboxClient.query(
          `SELECT * FROM pg_replication_slot_advance('${postgresSlot}', pg_current_wal_lsn());`,
        );
      } finally {
        await rootInboxClient.end();
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
      await handlerPool?.end();
      await startedEnv?.down();
    } catch (e) {
      inboxLogger.error(e);
      outboxLogger.error(e);
    }
  });

  describe('Outbox and inbox integration tests', () => {
    test('A single message is sent and received', async () => {
      // Arrange
      const entityId = uuid();
      const content = createContent(entityId);
      let inboxMessageReceived: StoredTransactionalMessage | undefined;
      const inboxMessageHandler = {
        aggregateType,
        messageType,
        handle: async (
          message: StoredTransactionalMessage,
          _client: DatabaseClient,
        ): Promise<void> => {
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
      await insertSourceEntity(
        handlerPool,
        entityId,
        content,
        storeOutboxMessage,
      );

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
      const receivedInboxMessages: StoredTransactionalMessage[] = [];
      const inboxMessageHandler = {
        aggregateType,
        messageType,
        handle: async (
          message: StoredTransactionalMessage,
          _client: DatabaseClient,
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
      await executeTransaction(await handlerPool.connect(), async (client) => {
        await Promise.all(
          ids.map(async (id) => {
            const entity = await client.query(
              `INSERT INTO public.source_entities (id, content) VALUES ($1, $2) RETURNING id, content;`,
              [id, createContent(id)],
            );
            expect(entity.rowCount).toBe(1);
            const message: TransactionalMessage = {
              id: uuid(),
              aggregateId: id,
              aggregateType,
              messageType,
              payload: entity.rows[0],
              metadata,
              createdAt: new Date().toISOString(),
            };
            await storeOutboxMessage(message, client);
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
      let inboxMessageReceived: StoredTransactionalMessage | undefined;
      const inboxMessageHandler = {
        aggregateType,
        messageType,
        handle: async (
          message: StoredTransactionalMessage,
          _client: DatabaseClient,
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
      await insertSourceEntity(
        handlerPool,
        entityId,
        content,
        storeOutboxMessage,
      );

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
      const inboxMessageReceived: StoredTransactionalMessage[] = [];
      let maxErrors = 10;
      const inboxMessageHandler = {
        aggregateType,
        messageType,
        handle: async (
          message: StoredTransactionalMessage,
          _client: DatabaseClient,
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
          handlerPool,
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

    test('Two messages are processed in order even if the first takes longer.', async () => {
      // Arrange
      const uuid1 = uuid();
      const uuid2 = uuid();
      const inboxMessageReceived: StoredTransactionalMessage[] = [];
      const inboxMessageHandler = {
        aggregateType,
        messageType,
        handle: async (
          message: StoredTransactionalMessage,
          _client: DatabaseClient,
        ): Promise<void> => {
          if (message.aggregateId === uuid1) {
            await sleep(250);
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
      await sleep(1);

      // Act
      await insertSourceEntity(
        handlerPool,
        uuid1,
        JSON.stringify({ id: uuid1, content: 'movie' }),
        storeOutboxMessage,
      );
      await insertSourceEntity(
        handlerPool,
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
  });

  describe('Inbox only integration tests', () => {
    test('With full concurrent processing and a slow first and fast second message the acknowledgement is only sent after the first is also done.', async () => {
      // Arrange
      const msg1 = createMsg();
      const msg2 = createMsg();

      // Act
      const storeInboxMessage = initializeMessageStorage(
        configs.inboxConfig,
        inboxLogger,
      );

      await executeTransaction(await handlerPool.connect(), async (client) => {
        await storeInboxMessage(msg1, client);
        await storeInboxMessage(msg2, client);
      });

      const items: string[] = [];
      const [shutdownInboxSrv] = initializeReplicationMessageListener(
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
        {
          concurrencyStrategy: createReplicationFullConcurrencyController(),
        },
      );
      cleanup = async () => {
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
      const findRegex = (msg: TransactionalMessage) =>
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

    test('An inbox message is acknowledged if there is no handler for it.', async () => {
      // Arrange
      const msg1 = createMsg({
        metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
      });
      const msg2 = createMsg({
        metadata: { routingKey: 'test.route', exchange: 'test-exchange' },
        messageType: 'something_else',
      });
      const processedMessages: StoredTransactionalMessage[] = [];
      const storeInboxMessage = initializeMessageStorage(
        configs.inboxConfig,
        inboxLogger,
      );
      const [shutdownInboxSrv] = initializeReplicationMessageListener(
        configs.inboxConfig,
        [
          {
            aggregateType,
            messageType,
            handle: async (
              message: StoredTransactionalMessage,
            ): Promise<void> => {
              processedMessages.push(message);
            },
          },
        ],
        inboxLogger,
      );
      cleanup = async () => {
        await shutdownInboxSrv();
      };

      // Act
      await executeTransaction(await handlerPool.connect(), async (client) => {
        await storeInboxMessage(msg1, client);
        await storeInboxMessage(msg2, client);
      });

      // Assert
      await sleep(500);
      expect(processedMessages).toHaveLength(1);
      expect(processedMessages[0]).toMatchObject(msg1);

      // Check that the second message is not received now either
      await shutdownInboxSrv();
      let receivedMsg2: StoredTransactionalMessage | null = null;
      const [shutdownInboxSrv2] = initializeReplicationMessageListener(
        configs.inboxConfig,
        [
          {
            aggregateType,
            messageType: 'something_else',
            handle: async (
              message: StoredTransactionalMessage,
            ): Promise<void> => {
              receivedMsg2 = message;
            },
          },
        ],
        inboxLogger,
      );
      cleanup = async () => {
        await shutdownInboxSrv2();
      };
      await sleep(500);
      expect(receivedMsg2).toBeNull();
    });

    test('An inbox message is is retried up to 5 times if it throws an error.', async () => {
      // Arrange
      const msg: TransactionalMessage = {
        id: uuid(),
        aggregateId: uuid(),
        aggregateType,
        messageType,
        payload: { content: 'some movie' },
        metadata,
        createdAt: '2023-01-18T21:02:27.000Z',
      };
      const storeInboxMessage = initializeMessageStorage(
        configs.inboxConfig,
        inboxLogger,
      );
      let inboxHandlerCounter = 0;
      const [shutdownInboxSrv] = initializeReplicationMessageListener(
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
      );
      cleanup = async () => {
        await shutdownInboxSrv();
      };

      // Act
      await executeTransaction(await handlerPool.connect(), async (client) => {
        await storeInboxMessage(msg, client);
      });

      // Assert
      await sleep(1000);
      expect(inboxHandlerCounter).toBe(5);
      const inboxResult = await handlerPool.query(
        `SELECT finished_attempts FROM ${configs.inboxConfig.settings.dbSchema}.${configs.inboxConfig.settings.dbTable} WHERE id = $1;`,
        [msg.id],
      );
      expect(inboxResult.rowCount).toBe(1);
      expect(inboxResult.rows[0].finished_attempts).toBe(5);
    });

    test('discriminating mutex cares for the correct message processing order', async () => {
      // Arrange
      const msg1 = createMsg({ aggregateId: '30', aggregateType: 'A' });
      const msg2 = createMsg({ aggregateId: '100', aggregateType: 'A' });
      const msg3 = createMsg({ aggregateId: '50', aggregateType: 'B' });
      const msg4 = createMsg({ aggregateId: '50', aggregateType: 'B' });
      const msg5 = createMsg({ aggregateId: '1', aggregateType: 'C' });
      const msg6 = createMsg({ aggregateId: '1', aggregateType: 'C' });
      const createHandler = (aggregateType: string) => ({
        aggregateType,
        messageType,
        handle: async (message: TransactionalMessage): Promise<void> => {
          await sleep(Number(message.aggregateId));
          items.push(message);
        },
      });

      // Act
      const storeInboxMessage = initializeMessageStorage(
        configs.inboxConfig,
        inboxLogger,
      );

      await executeTransaction(await handlerPool.connect(), async (client) => {
        await storeInboxMessage(msg1, client);
        await storeInboxMessage(msg2, client);
        await storeInboxMessage(msg3, client);
        await storeInboxMessage(msg4, client);
        await storeInboxMessage(msg5, client);
        await storeInboxMessage(msg6, client);
      });

      const items: TransactionalMessage[] = [];
      const [shutdownInboxSrv] = initializeReplicationMessageListener(
        configs.inboxConfig,
        [createHandler('A'), createHandler('B'), createHandler('C')],
        inboxLogger,
        {
          concurrencyStrategy:
            createReplicationDiscriminatingMutexConcurrencyController(
              (message) => message.aggregateType,
            ),
        },
      );
      cleanup = async () => {
        await shutdownInboxSrv();
      };

      // Assert
      const duration = await sleepUntilTrue(() => items.length === 6, 500_000);
      expect(items[0]).toMatchObject(msg5);
      expect(items[1]).toMatchObject(msg6);
      expect(items[2]).toMatchObject(msg1);
      expect(items[3]).toMatchObject(msg3);
      expect(items[4]).toMatchObject(msg4);
      expect(items[5]).toMatchObject(msg2);
      expect(duration).toBeGreaterThan(130);
    });

    test('multi concurrency controller cares for the correct message processing order', async () => {
      // Arrange - message execution should all sum up to 40ms per concurrency type

      // discriminating tasks (1 then 2 and in parallel 3 then 4)
      const disc1 = createMsg({
        aggregateId: '20',
        aggregateType: 'A',
        messageType: 'X',
      });
      const disc2 = createMsg({
        aggregateId: '20',
        aggregateType: 'A',
        messageType: 'X',
      });
      const disc3 = createMsg({
        aggregateId: '20',
        aggregateType: 'A',
        messageType: 'Y',
      });
      const disc4 = createMsg({
        aggregateId: '20',
        aggregateType: 'A',
        messageType: 'Y',
      });

      // concurrent tasks (1+2in parallel)
      const conc1 = createMsg({ aggregateId: '40', aggregateType: 'B' });
      const conc2 = createMsg({ aggregateId: '40', aggregateType: 'B' });

      // mutex tasks (1 then 2)
      const mut1 = createMsg({ aggregateId: '20', aggregateType: 'C' });
      const mut2 = createMsg({ aggregateId: '20', aggregateType: 'C' });

      // semaphore tasks (1 and 2 in parallel then 3)
      const sem1 = createMsg({ aggregateId: '20', aggregateType: 'D' });
      const sem2 = createMsg({ aggregateId: '30', aggregateType: 'D' });
      const sem3 = createMsg({ aggregateId: '20', aggregateType: 'D' });

      const createHandler = (
        msgArray: TransactionalMessage[],
        aggType: string,
        msgType?: string,
      ) => ({
        aggregateType: aggType,
        messageType: msgType ?? messageType,
        handle: async (message: TransactionalMessage): Promise<void> => {
          await sleep(Number(message.aggregateId));
          msgArray.push(message);
        },
      });

      const concurrencyStrategy = createReplicationMultiConcurrencyController(
        (message) => {
          switch (message.aggregateType) {
            case 'A':
              return 'discriminating-mutex';
            case 'B':
              return 'full-concurrency';
            case 'C':
              return 'mutex';
            case 'D':
              return 'semaphore';
            default:
              throw new Error('unreachable');
          }
        },
        {
          discriminator: (message) => message.messageType,
          maxSemaphoreParallelism: 2,
        },
      );
      const discAXMessages: TransactionalMessage[] = [];
      const discAYMessages: TransactionalMessage[] = [];
      const concMessages: TransactionalMessage[] = [];
      const mutMessages: TransactionalMessage[] = [];
      const semMessages: TransactionalMessage[] = [];

      // Act
      const storeInboxMessage = initializeMessageStorage(
        configs.inboxConfig,
        inboxLogger,
      );

      await executeTransaction(await handlerPool.connect(), async (client) => {
        await storeInboxMessage(disc1, client);
        await storeInboxMessage(disc2, client);
        await storeInboxMessage(disc3, client);
        await storeInboxMessage(disc4, client);
        await storeInboxMessage(conc1, client);
        await storeInboxMessage(conc2, client);
        await storeInboxMessage(mut1, client);
        await storeInboxMessage(mut2, client);
        await storeInboxMessage(sem1, client);
        await storeInboxMessage(sem2, client);
        await storeInboxMessage(sem3, client);
      });

      const [shutdownInboxSrv] = initializeReplicationMessageListener(
        configs.inboxConfig,
        [
          createHandler(discAXMessages, 'A', 'X'),
          createHandler(discAYMessages, 'A', 'Y'),
          createHandler(concMessages, 'B'),
          createHandler(mutMessages, 'C'),
          createHandler(semMessages, 'D'),
        ],
        inboxLogger,
        {
          concurrencyStrategy,
        },
      );
      cleanup = async () => {
        await shutdownInboxSrv();
      };

      // Assert
      const duration = await sleepUntilTrue(
        () =>
          discAXMessages.length === 2 &&
          discAYMessages.length === 2 &&
          concMessages.length === 2 &&
          mutMessages.length === 2 &&
          semMessages.length === 3,
        50_000,
      );
      expect(discAXMessages[0]).toMatchObject(disc1);
      expect(discAXMessages[1]).toMatchObject(disc2);
      expect(discAYMessages[0]).toMatchObject(disc3);
      expect(discAYMessages[1]).toMatchObject(disc4);
      expect(concMessages.map((m) => m.id).sort()).toStrictEqual(
        [conc1.id, conc2.id].sort(),
      );
      expect(mutMessages[0]).toMatchObject(mut1);
      expect(mutMessages[1]).toMatchObject(mut2);
      expect(semMessages[0]).toMatchObject(sem1);
      expect(semMessages[1]).toMatchObject(sem2);
      expect(semMessages[2]).toMatchObject(sem3);
      expect(duration).toBeGreaterThan(40);
    });
  });

  describe('Outbox only integration tests', () => {
    test('A single outbox message is stored and received in the outbox listener even with two outbox listeners', async () => {
      // Arrange
      const entityId = uuid();
      const content = createContent(entityId);
      let receivedFromOutbox1: TransactionalMessage | null = null;
      let receivedFromOutbox2: TransactionalMessage | null = null;
      const storeOutboxMessage = initializeMessageStorage(
        configs.outboxConfig,
        outboxLogger,
      );
      const [shutdown1] = initializeReplicationMessageListener(
        configs.outboxConfig,
        {
          handle: async (msg) => {
            receivedFromOutbox1 = msg;
          },
        },
        outboxLogger,
      );
      await sleep(500); // enough time for the first one to start up
      const [shutdown2] = initializeReplicationMessageListener(
        configs.outboxConfig,
        {
          handle: async (msg) => {
            receivedFromOutbox2 = msg;
          },
        },
        outboxLogger,
      );

      // Act
      await insertSourceEntity(
        handlerPool,
        entityId,
        content,
        storeOutboxMessage,
      );

      // Assert
      await sleep(500);
      expect(receivedFromOutbox1).not.toBeNull();
      // The second listener does not start as only one reader per slot is allowed
      expect(receivedFromOutbox2).toBeNull();
      const log = outboxLogs.filter(
        (log) =>
          log.args[1] ===
          'The replication slot for the outbox listener is currently in use.',
      );
      expect(log.length).toBeGreaterThanOrEqual(1);

      cleanup = async () => {
        await shutdown1();
        await shutdown2();
      };
    });
  });
});

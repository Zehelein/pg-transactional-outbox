// eslint-disable-next-line prettier/prettier
// import * as wtf from 'wtfnode';
// use `wtf.dump();` to get all open handles
/* eslint-disable @typescript-eslint/no-empty-function */
import { resolve } from 'path';
import { Client, Pool } from 'pg';
import {
  DatabaseClient,
  InMemoryLogEntry,
  MessageStorage,
  StoredTransactionalMessage,
  TransactionalLogger,
  TransactionalMessage,
  TransactionalMessageHandler,
  executeTransaction,
  getInMemoryLogger,
  initializeMessageStorage,
  initializePollingMessageListener,
  runScheduledMessageCleanup,
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
  setupPollingTestDb,
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
  const [inSrvShutdown] = initializePollingMessageListener(
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
  const [outSrvShutdown] = initializePollingMessageListener(
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
  overrides?: Partial<StoredTransactionalMessage>,
): TransactionalMessage => ({
  id: uuid(),
  aggregateId: uuid(),
  aggregateType,
  messageType,
  payload: { content: 'some movie' },
  metadata,
  ...overrides,
});

const insertSourceEntity = async (
  handlerPool: Pool,
  id: string,
  payload: string,
  storeOutboxMessage: MessageStorage,
) => {
  await executeTransaction(await handlerPool.connect(), async (client) => {
    const entity = await client.query(
      `INSERT INTO public.source_entities (id, content) VALUES ($1, $2) RETURNING id, content;`,
      [id, payload],
    );
    if (entity.rowCount !== 1) {
      throw new Error(
        `Inserted ${entity.rowCount} source entities instead of 1.`,
      );
    }
    await storeMessage(
      client,
      id,
      JSON.stringify({ id, content: createContent(id) }),
      storeOutboxMessage,
    );
  });
};

const storeMessage = async (
  client: DatabaseClient,
  id: string,
  payload: string,
  storeOutboxMessage: MessageStorage,
) => {
  const message: TransactionalMessage = {
    id: uuid(),
    aggregateId: id,
    aggregateType,
    messageType,
    payload,
    metadata,
    createdAt: new Date().toISOString(),
  };
  await storeOutboxMessage(message, client);
};

describe('Polling integration tests', () => {
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
      'docker-compose-polling.yml',
    );
    startedEnv = await dockerEnv.up();

    const postgresContainer = startedEnv.getContainer('postgres-polling');
    const port = postgresContainer.getMappedPort(5432);
    configs = getConfigs(port);
    await setupPollingTestDb(configs);

    handlerPool = new Pool(configs.handlerConnection);
    handlerPool.on('error', (err) => {
      outboxLogger.error(err, 'PostgreSQL pool error');
      inboxLogger.error(err, 'PostgreSQL pool error');
    });
  });

  beforeEach(async () => {
    inboxLogs.length = 0;
    outboxLogs.length = 0;

    const rootInboxClient = new Client({
      ...configs.handlerConnection,
      user: 'postgres',
      password: 'postgres',
    });
    try {
      await rootInboxClient.connect();
      await rootInboxClient.query(
        `DELETE FROM ${configs.inboxConfig.settings.dbSchema}.${configs.inboxConfig.settings.dbTable};
         DELETE FROM ${configs.outboxConfig.settings.dbSchema}.${configs.outboxConfig.settings.dbTable};`,
      );
    } finally {
      await rootInboxClient.end();
    }
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
              concurrency: 'sequential',
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

    test(
      '10000 messages are reliably sent and received in the same order.',
      async () => {
        const totalCount = 10_000;
        // Arrange
        const uuids = Array.from(Array(totalCount), () => uuid());
        const inboxMessageReceived: StoredTransactionalMessage[] = [];
        const inboxMessageHandler = {
          aggregateType,
          messageType,
          handle: async (
            message: StoredTransactionalMessage,
            _client: DatabaseClient,
          ): Promise<void> => {
            await sleep(1);
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

        // Act
        for (const id of uuids) {
          // Sequentially insert the messages to test message processing sort order
          await storeMessage(
            handlerPool,
            id,
            JSON.stringify({ id, content: 'movie' }),
            storeOutboxMessage,
          );
          await sleep(1); // at least one millisecond between saves to guarantee sorting
        }

        // Assert
        const timeout = Date.now() + 4 * 60 * 1000;
        let lastLength = 0;
        while (
          inboxMessageReceived.length !== totalCount &&
          Date.now() < timeout
        ) {
          if (inboxMessageReceived.length > lastLength) {
            lastLength = inboxMessageReceived.length;
          }
          await sleep(10);
        }
        expect(timeout).toBeGreaterThanOrEqual(Date.now());
        expect(inboxMessageReceived).toHaveLength(totalCount);
        expect(
          inboxMessageReceived.map((msg) => msg.aggregateId),
        ).toStrictEqual(uuids);
      },
      5 * 60 * 1000,
    );

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
    test('With full concurrent processing and a slow first and fast second message is only finished after the first is done.', async () => {
      // Arrange
      const msg1 = createMsg({ concurrency: 'parallel' });
      const msg2 = createMsg({ concurrency: 'parallel' });

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
      const [shutdownInboxSrv] = initializePollingMessageListener(
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
          batchSizeStrategy: () => Promise.resolve(2),
          messageProcessingTimeoutStrategy: () => 500,
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
      await sleep(100);
      const findRegex = (msg: TransactionalMessage) =>
        new RegExp(
          `Finished processing the message with id ${msg.id} and type ${msg.messageType}.`,
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
      expect(msg2Index).toBeGreaterThan(0);
      expect(msg1Index).toBeGreaterThan(msg2Index);
    });

    test('An inbox message is marked as processed if there is no handler for it.', async () => {
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
      const [shutdownInboxSrv] = initializePollingMessageListener(
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
      const [shutdownInboxSrv2] = initializePollingMessageListener(
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
      const [shutdownInboxSrv] = initializePollingMessageListener(
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
      await sleep(2_000);
      inboxLogs;
      expect(inboxHandlerCounter).toBe(5);
      const inboxResult = await handlerPool.query(
        `SELECT finished_attempts FROM ${configs.inboxConfig.settings.dbSchema}.${configs.inboxConfig.settings.dbTable} WHERE id = $1;`,
        [msg.id],
      );
      expect(inboxResult.rowCount).toBe(1);
      expect(inboxResult.rows[0].finished_attempts).toBe(5);
    });

    test('Segmented messages care for the correct message processing order', async () => {
      // Arrange
      const msg1 = createMsg({ aggregateId: '30', segment: 'A' });
      const msg2 = createMsg({ aggregateId: '100', segment: 'A' });
      const msg3 = createMsg({ aggregateId: '50', segment: 'B' });
      const msg4 = createMsg({ aggregateId: '50', segment: 'B' });
      const msg5 = createMsg({ aggregateId: '1', segment: 'C' });
      const msg6 = createMsg({ aggregateId: '1', segment: 'C' });

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
      const [shutdownInboxSrv] = initializePollingMessageListener(
        configs.inboxConfig,
        {
          handle: async (message: TransactionalMessage): Promise<void> => {
            await sleep(Number(message.aggregateId));
            items.push(message);
          },
        },
        inboxLogger,
        {
          batchSizeStrategy: () => Promise.resolve(3),
        },
      );
      cleanup = async () => {
        await shutdownInboxSrv();
      };

      // Assert
      const duration = await sleepUntilTrue(() => items.length === 6, 10_000);
      expect(items[0]).toMatchObject(msg5);
      expect(items[1]).toMatchObject(msg6);
      expect(items[2]).toMatchObject(msg1);
      expect(items[3]).toMatchObject(msg3);
      expect(items[4]).toMatchObject(msg4);
      expect(items[5]).toMatchObject(msg2);
      expect(duration).toBeGreaterThan(130);
    });

    test('multi concurrency controller cares for the correct message processing order', async () => {
      // Arrange

      const a1 = createMsg({ segment: 'A', concurrency: 'sequential' });
      const a2 = createMsg({ segment: 'A', concurrency: 'parallel' });
      const a3 = createMsg({ segment: 'A', concurrency: 'sequential' });
      const a4 = createMsg({ segment: 'A', concurrency: 'parallel' });
      const a5 = createMsg({ segment: 'A', concurrency: 'sequential' });

      const b1 = createMsg({ segment: 'B', concurrency: 'sequential' });
      const b2 = createMsg({ segment: 'B', concurrency: 'sequential' });
      const b3 = createMsg({ segment: 'B', concurrency: 'sequential' });
      const b4 = createMsg({ segment: 'B', concurrency: 'sequential' });
      const b5 = createMsg({ segment: 'B', concurrency: 'sequential' });

      const c1 = createMsg({ segment: 'C', concurrency: 'parallel' });
      const c2 = createMsg({ segment: 'C', concurrency: 'parallel' });
      const c3 = createMsg({ segment: 'C', concurrency: 'parallel' });
      const c4 = createMsg({ segment: 'C', concurrency: 'parallel' });
      const c5 = createMsg({ segment: 'C', concurrency: 'sequential' });

      const d1 = createMsg({ segment: 'D', concurrency: 'sequential' });
      const d2 = createMsg({ segment: 'D', concurrency: 'parallel' });
      const d3 = createMsg({ segment: 'D', concurrency: 'parallel' });
      const d4 = createMsg({ segment: 'D', concurrency: 'parallel' });
      const d5 = createMsg({ segment: 'D', concurrency: 'parallel' });

      const aMessages: TransactionalMessage[] = [];
      const bMessages: TransactionalMessage[] = [];
      const cMessages: TransactionalMessage[] = [];
      const dMessages: TransactionalMessage[] = [];

      // Act
      const storeInboxMessage = initializeMessageStorage(
        configs.inboxConfig,
        inboxLogger,
      );

      await executeTransaction(await handlerPool.connect(), async (client) => {
        await storeInboxMessage(a1, client);
        await storeInboxMessage(a2, client);
        await storeInboxMessage(a3, client);
        await storeInboxMessage(a4, client);
        await storeInboxMessage(a5, client);
        await storeInboxMessage(b1, client);
        await storeInboxMessage(b2, client);
        await storeInboxMessage(b3, client);
        await storeInboxMessage(b4, client);
        await storeInboxMessage(b5, client);
        await storeInboxMessage(c1, client);
        await storeInboxMessage(c2, client);
        await storeInboxMessage(c3, client);
        await storeInboxMessage(c4, client);
        await storeInboxMessage(c5, client);
        await storeInboxMessage(d1, client);
        await storeInboxMessage(d2, client);
        await storeInboxMessage(d3, client);
        await storeInboxMessage(d4, client);
        await storeInboxMessage(d5, client);
      });

      const [shutdownInboxSrv] = initializePollingMessageListener(
        configs.inboxConfig,
        {
          handle: async (message: TransactionalMessage): Promise<void> => {
            await sleep(5);
            switch (message.segment) {
              case 'A':
                aMessages.push(message);
                break;
              case 'B':
                bMessages.push(message);
                break;
              case 'C':
                cMessages.push(message);
                break;
              case 'D':
                dMessages.push(message);
                break;
            }
          },
        },
        inboxLogger,
      );
      cleanup = async () => {
        await shutdownInboxSrv();
      };

      // Assert
      await sleepUntilTrue(
        () =>
          aMessages.length === 5 &&
          bMessages.length === 5 &&
          cMessages.length === 5 &&
          dMessages.length === 5,
        30_000,
      );
      const filterMap = (messages: TransactionalMessage[]) =>
        messages.filter((m) => m.concurrency === 'sequential').map((m) => m.id);
      if (aMessages[0].id !== a1.id) {
        inboxLogs;
      }
      expect(filterMap(aMessages)).toStrictEqual([a1.id, a3.id, a5.id]);
      expect(filterMap(bMessages)).toStrictEqual([
        b1.id,
        b2.id,
        b3.id,
        b4.id,
        b5.id,
      ]);
      expect(filterMap(cMessages)).toStrictEqual([c5.id]);
      expect(filterMap(dMessages)).toStrictEqual([d1.id]);
    });

    test('cleanup deletes old processed messages correctly', async () => {
      // Arrange
      const cfg = configs.inboxConfig;
      const settings = configs.inboxConfig.settings;
      const m1 = createMsg();
      const m2 = createMsg();
      const m3 = createMsg();
      const m4 = createMsg();
      const storeInboxMessage = initializeMessageStorage(
        configs.inboxConfig,
        inboxLogger,
      );
      await executeTransaction(await handlerPool.connect(), async (client) => {
        const sql = /* sql */ `UPDATE ${settings.dbSchema}.${settings.dbTable} SET processed_at = $1 WHERE id = $2;`;
        const secondsOld = (seconds: number) =>
          new Date(Date.now() - seconds * 1000).toISOString();
        await storeInboxMessage(m1, client);
        client.query(sql, [secondsOld(200), m1.id]);
        await storeInboxMessage(m2, client);
        client.query(sql, [secondsOld(300), m2.id]);
        await storeInboxMessage(m3, client);
        client.query(sql, [secondsOld(400), m3.id]);
        await storeInboxMessage(m4, client);
        client.query(sql, [secondsOld(500), m4.id]);
      });
      const listenerPool = new Pool(cfg.dbListenerConfig);

      const timeout = runScheduledMessageCleanup(
        listenerPool,
        {
          ...cfg,
          settings: {
            ...settings,
            messageCleanupIntervalInMs: 100,
            messageCleanupProcessedInSec: 250,
            messageCleanupAbandonedInSec: undefined,
            messageCleanupAllInSec: undefined,
          },
        },
        inboxLogger,
      );

      cleanup = async () => {
        clearInterval(timeout);
        await listenerPool.end();
      };

      const getCount = async () =>
        (
          await handlerPool.query(
            /* sql */ `SELECT COUNT(*) as count FROM ${settings.dbSchema}.${settings.dbTable};`,
          )
        ).rows[0];
      expect((await getCount()).count).toBe('4');
      await sleep(200);
      expect((await getCount()).count).toBe('1');
    });

    test('cleanup deletes old abandoned messages correctly', async () => {
      // Arrange
      const cfg = configs.inboxConfig;
      const settings = configs.inboxConfig.settings;
      const m1 = createMsg();
      const m2 = createMsg();
      const m3 = createMsg();
      const m4 = createMsg();
      const storeInboxMessage = initializeMessageStorage(
        configs.inboxConfig,
        inboxLogger,
      );
      await executeTransaction(await handlerPool.connect(), async (client) => {
        const sql = /* sql */ `UPDATE ${settings.dbSchema}.${settings.dbTable} SET abandoned_at = $1 WHERE id = $2;`;
        const secondsOld = (seconds: number) =>
          new Date(Date.now() - seconds * 1000).toISOString();
        await storeInboxMessage(m1, client);
        client.query(sql, [secondsOld(200), m1.id]);
        await storeInboxMessage(m2, client);
        client.query(sql, [secondsOld(300), m2.id]);
        await storeInboxMessage(m3, client);
        client.query(sql, [secondsOld(400), m3.id]);
        await storeInboxMessage(m4, client);
        client.query(sql, [secondsOld(500), m4.id]);
      });
      const listenerPool = new Pool(cfg.dbListenerConfig);

      const timeout = runScheduledMessageCleanup(
        listenerPool,
        {
          ...cfg,
          settings: {
            ...settings,
            messageCleanupIntervalInMs: 100,
            messageCleanupProcessedInSec: undefined,
            messageCleanupAbandonedInSec: 450,
            messageCleanupAllInSec: undefined,
          },
        },
        inboxLogger,
      );

      cleanup = async () => {
        clearInterval(timeout);
        await listenerPool.end();
      };

      const getCount = async () =>
        (
          await handlerPool.query(
            /* sql */ `SELECT COUNT(*) as count FROM ${settings.dbSchema}.${settings.dbTable};`,
          )
        ).rows[0];
      expect((await getCount()).count).toBe('4');
      await sleep(200);
      expect((await getCount()).count).toBe('3');
    });

    test('cleanup deletes old messages correctly', async () => {
      // Arrange
      const cfg = configs.inboxConfig;
      const settings = configs.inboxConfig.settings;
      const secondsOld = (seconds: number) =>
        new Date(Date.now() - seconds * 1000).toISOString();
      const m1 = createMsg({ createdAt: secondsOld(200) });
      const m2 = createMsg({ createdAt: secondsOld(400) });
      const m3 = createMsg({ createdAt: secondsOld(300) });
      const m4 = createMsg({ createdAt: secondsOld(500) });
      const storeInboxMessage = initializeMessageStorage(
        configs.inboxConfig,
        inboxLogger,
      );
      await executeTransaction(await handlerPool.connect(), async (client) => {
        await storeInboxMessage(m1, client);
        await storeInboxMessage(m2, client);
        await storeInboxMessage(m3, client);
        await storeInboxMessage(m4, client);
      });
      const listenerPool = new Pool(cfg.dbListenerConfig);

      const timeout = runScheduledMessageCleanup(
        listenerPool,
        {
          ...cfg,
          settings: {
            ...settings,
            messageCleanupIntervalInMs: 100,
            messageCleanupProcessedInSec: undefined,
            messageCleanupAbandonedInSec: undefined,
            messageCleanupAllInSec: 350,
          },
        },
        inboxLogger,
      );

      cleanup = async () => {
        clearInterval(timeout);
        await listenerPool.end();
      };

      const getCount = async () =>
        (
          await handlerPool.query(
            /* sql */ `SELECT COUNT(*) as count FROM ${settings.dbSchema}.${settings.dbTable};`,
          )
        ).rows[0];
      expect((await getCount()).count).toBe('4');
      await sleep(200);
      expect((await getCount()).count).toBe('2');
    });

    test('An inbox message that has a timeout will not throw a promise rejected error and retry 5 times.', async () => {
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
      const [shutdownInboxSrv] = initializePollingMessageListener(
        {
          ...configs.inboxConfig,
          settings: {
            ...configs.inboxConfig.settings,
            messageProcessingTimeoutInMs: 250,
          },
        },
        [
          {
            aggregateType,
            messageType,
            handle: async (_, client): Promise<void> => {
              await client.query('SELECT 1;');
              await sleep(500);
              expect((client as any)._ending || (client as any)._ended).toBe(
                true,
              );
              await client.query('SELECT 1;');
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
      await sleep(2_000);
      inboxLogs;
      const inboxResult = await handlerPool.query(
        `SELECT finished_attempts FROM ${configs.inboxConfig.settings.dbSchema}.${configs.inboxConfig.settings.dbTable} WHERE id = $1;`,
        [msg.id],
      );
      expect(inboxResult.rowCount).toBe(1);
      expect(inboxResult.rows[0].finished_attempts).toBe(5);
      expect(inboxLogs[inboxLogs.length - 1].args[1]).toMatch(
        'Giving up processing the inbox message with id ',
      );
    });
  });
});

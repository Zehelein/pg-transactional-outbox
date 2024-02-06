import { Pool, PoolClient } from 'pg';
import {
  IsolationLevel,
  PollingConfig,
  ReplicationConfig,
  TransactionalMessage,
  TransactionalMessageHandler,
  createReplicationMultiConcurrencyController,
  ensureExtendedError,
  executeTransaction,
  getDefaultLogger,
  initializeMessageStorage,
  initializePollingMessageListener,
  initializeReplicationMessageListener,
} from 'pg-transactional-outbox';

/** The main entry point of the message producer. */
(async () => {
  const logger = getDefaultLogger('outbox');

  // Configuration settings for the replication and inbox table configurations

  // This configuration is used to start a transaction that locks and updates
  // the row in the inbox table that was found from the inbox table. This connection
  // will also be used in the message handler so every select and data change is
  // part of the same database transaction. The inbox database row is then
  // marked as "processed" when everything went fine.
  const dbHandlerConfig = {
    host: 'localhost',
    port: 5432,
    user: 'db_inbox_handler',
    password: 'db_inbox_handler_password',
    database: 'pg_transactional_inbox',
  };
  // Configure the replication role to receive notifications when a new inbox
  // row was added to the inbox table. This role must have the replication
  // permission.
  const dbListenerConfig = {
    host: 'localhost',
    port: 5432,
    user: 'db_inbox_listener',
    password: 'db_inbox_listener_password',
    database: 'pg_transactional_inbox',
  };
  const baseSettings = {
    dbSchema: 'public',
    dbTable: 'outbox',
  };
  const replicationConfig: ReplicationConfig = {
    outboxOrInbox: 'inbox',
    dbHandlerConfig,
    dbListenerConfig,
    settings: {
      ...baseSettings,
      postgresPub: 'pg_transactional_inbox_pub',
      postgresSlot: 'pg_transactional_outbox_slot',
    },
  };
  const pollingConfig: PollingConfig = {
    outboxOrInbox: 'inbox',
    dbListenerConfig,
    settings: {
      ...baseSettings,
      nextMessagesBatchSize: 5,
      nextMessagesFunctionName: 'next_inbox_messages',
      nextMessagesPollingInterval: 250,
    },
  };

  // Create the database pool to store the incoming inbox messages
  const pool = new Pool(replicationConfig.dbHandlerConfig);
  pool.on('error', (err) => {
    logger.error(ensureExtendedError(err, 'DB_ERROR'), 'PostgreSQL pool error');
  });

  // Initialize the inbox message storage to store incoming messages in the inbox
  const storeInboxMessage = initializeMessageStorage(replicationConfig, logger);

  // Initialize the message receiver e.g. based on RabbitMQ
  // In the simplest scenario use the inter process communication:
  process.on('message', async (message: TransactionalMessage) => {
    await executeTransaction(
      await pool.connect(),
      async (client): Promise<void> => {
        await storeInboxMessage(message, client);
      },
      IsolationLevel.ReadCommitted,
    );
  });

  // Define an optional concurrency strategy to handle messages with the message
  // type "ABC" in parallel while handling other messages sequentially per
  // aggregate type and message type combination.
  const concurrencyStrategy = createReplicationMultiConcurrencyController(
    (message) => {
      switch (message.messageType) {
        case 'ABC':
          return 'full-concurrency';
        default:
          return 'discriminating-mutex';
      }
    },
    {
      discriminator: (message) =>
        `${message.aggregateType}.${message.messageType}`,
    },
  );

  // Declare the message handler
  const movieCreatedHandler: TransactionalMessageHandler = {
    aggregateType: 'movie',
    messageType: 'movie_created',
    handle: async (
      message: TransactionalMessage,
      client: PoolClient,
    ): Promise<void> => {
      // Executes the message handler logic using the same database
      // transaction as the inbox message acknowledgement.
      const { payload } = message;
      if (
        typeof payload === 'object' &&
        payload !== null &&
        'id' in payload &&
        typeof payload.id === 'string' &&
        'title' in payload &&
        typeof payload.title === 'string'
      ) {
        await client.query(
          `INSERT INTO public.published_movies (id, title) VALUES ($1, $2)`,
          [payload.id, payload.title],
        );
      }
    },
    handleError: async (
      error: Error,
      message: TransactionalMessage,
      _client: PoolClient,
      retry: boolean,
    ): Promise<void> => {
      if (!retry) {
        // Potentially send a compensating message to adjust other services e.g. via the Saga Pattern
        logger.error(
          error,
          `Giving up processing message with ID ${message.id}.`,
        );
      }
    },
  };

  // Initialize and start the inbox listener
  let shutdownListener: () => Promise<void>;
  if (process.env.listenerType === 'replication') {
    const [shutdown] = initializeReplicationMessageListener(
      replicationConfig,
      [movieCreatedHandler],
      logger,
      {
        concurrencyStrategy,
        messageProcessingTimeoutStrategy: (message: TransactionalMessage) =>
          message.messageType === 'ABC' ? 10_000 : 2_000,
        messageProcessingTransactionLevelStrategy: (
          message: TransactionalMessage,
        ) =>
          message.messageType === 'ABC'
            ? IsolationLevel.ReadCommitted
            : IsolationLevel.RepeatableRead,
      },
    );
    shutdownListener = shutdown;
  } else {
    const [shutdown] = initializePollingMessageListener(
      pollingConfig,
      [movieCreatedHandler],
      logger,
      {
        messageProcessingTimeoutStrategy: (message: TransactionalMessage) =>
          message.messageType === 'ABC' ? 10_000 : 2_000,
        messageProcessingTransactionLevelStrategy: (
          message: TransactionalMessage,
        ) =>
          message.messageType === 'ABC'
            ? IsolationLevel.ReadCommitted
            : IsolationLevel.RepeatableRead,
      },
    );
    shutdownListener = shutdown;
  }
  await shutdownListener();
})();

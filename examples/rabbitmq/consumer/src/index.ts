import process from 'node:process';
import { Pool } from 'pg';
import {
  GeneralMessageHandler,
  PollingConfig,
  ReplicationConfig,
  TransactionalMessage,
  createReplicationMutexConcurrencyController,
  getDefaultLogger,
  initializeMessageStorage,
  initializePollingMessageListener,
  initializeReplicationMessageListener,
} from 'pg-transactional-outbox';

(async () => {
  const logger = getDefaultLogger('outbox');

  // Initialize the actual message publisher e.g. publish the message via RabbitMQ
  const messagePublisher: GeneralMessageHandler = {
    handle: async (message: TransactionalMessage): Promise<void> => {
      // In the simplest case the message can be sent via inter process communication
      process.send?.(message);
    },
  };

  const dbListenerConfig = {
    host: 'localhost',
    port: 5432,
    user: 'db_outbox_listener',
    password: 'db_outbox_listener_password',
    database: 'pg_transactional_outbox',
  };
  const baseSettings = {
    dbSchema: 'public',
    dbTable: 'outbox',
  };
  const replicationConfig: ReplicationConfig = {
    outboxOrInbox: 'outbox',
    dbListenerConfig,
    settings: {
      ...baseSettings,
      postgresPub: 'pg_transactional_outbox_pub',
      postgresSlot: 'pg_transactional_outbox_slot',
    },
  };
  const pollingConfig: PollingConfig = {
    outboxOrInbox: 'outbox',
    dbListenerConfig,
    settings: {
      ...baseSettings,
      nextMessagesBatchSize: 5,
      nextMessagesFunctionName: 'next_outbox_messages',
      nextMessagesPollingInterval: 250,
    },
  };

  // Initialize and start the listening for outbox messages. This listeners
  // receives all the outbox table inserts from the WAL or via polling. It
  // executes the messagePublisher handle function with every received outbox
  // message. It cares for the at least once delivery.
  let shutdownListener: () => Promise<void>;
  if (process.env.listenerType === 'replication') {
    const [shutdown] = initializeReplicationMessageListener(
      replicationConfig,
      messagePublisher,
      logger,
      {
        concurrencyStrategy: createReplicationMutexConcurrencyController(),
        messageProcessingTimeoutStrategy: (message: TransactionalMessage) =>
          message.messageType === 'ABC' ? 10_000 : 2_000,
      },
    );
    shutdownListener = shutdown;
  } else {
    const [shutdown] = initializePollingMessageListener(
      pollingConfig,
      messagePublisher,
      logger,
      {
        messageProcessingTimeoutStrategy: (message: TransactionalMessage) =>
          message.messageType === 'ABC' ? 10_000 : 2_000,
      },
    );
    shutdownListener = shutdown;
  }

  // Initialize the message storage function to store outbox messages. It will
  // be called to insert the outgoing message into the outbox table as part of
  // the DB transaction that is responsible for this event.
  const storeOutboxMessage = initializeMessageStorage(
    replicationConfig,
    logger,
  );

  // The actual business logic generates in this example a new movie in the DB
  // and wants to reliably send a "movie_created" message.
  const pool = new Pool({
    host: 'localhost',
    port: 5432,
    user: 'db_outbox_handler',
    password: 'db_outbox_handler_password',
    database: 'pg_transactional_outbox',
  });
  const client = await pool.connect();

  try {
    // The movie and the outbox message must be inserted in the same transaction.
    await client.query('START TRANSACTION ISOLATION LEVEL SERIALIZABLE');
    // Insert the movie (and query/mutate potentially a lot more data)
    const result = await client.query(
      `INSERT INTO public.movies (title) VALUES ('some movie') RETURNING id, title;`,
    );
    // Define the outbox message
    const message: TransactionalMessage = {
      id: new Crypto().randomUUID(),
      aggregateType: 'movie',
      messageType: 'movie_created',
      aggregateId: result.rows[0].id,
      payload: result.rows[0],
      createdAt: new Date().toISOString(),
    };
    // Store the message in the outbox table
    await storeOutboxMessage(message, client);
    // (Try to) commit the transaction to save the movie and the outbox message
    await client.query('COMMIT');
    client.release();
  } catch (err) {
    // In case of an error roll back the DB transaction - neither movie nor
    // the outbox message will be stored in the DB now.
    await client?.query('ROLLBACK');
    client.release(true);
    throw err;
  }
  await shutdownListener();
})();

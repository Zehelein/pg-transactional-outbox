# pg-transactional-outbox

The `pg-transactional-outbox` library implements the transactional outbox and
transactional inbox pattern based on PostgreSQL. It ensures that outgoing
messages are only sent when your code successfully commits your transaction. On
the receiving side, it ensures that a message is stored exactly once and that
your handler marks the message only as done when your code finishes
successfully.

![postgresql_outbox_pattern](https://user-images.githubusercontent.com/9946441/211221740-a10d3c0b-dfa9-4c4e-84fb-068f6e63aaac.jpeg)
_Message delivery via the PostgreSQL-based transactional outbox pattern
(generated with Midjourney)_

Install the library
[npmjs | pg-transactional-outbox](https://www.npmjs.com/package/pg-transactional-outbox)
e.g. via

```
npm i pg-transactional-outbox
```

or

```
yarn add pg-transactional-outbox
```

**Table of contents:**

- [Context](#context)
- [The patterns](#the-patterns)
  - [What is the transactional outbox pattern](#what-is-the-transactional-outbox-pattern)
  - [What is the transactional inbox pattern](#what-is-the-transactional-inbox-pattern)
  - [What is the PostgreSQL Logical Replication](#what-is-the-postgresql-logical-replication)
- [The pg-transactional-outbox library usage](#the-pg-transactional-outbox-library-overview)
  - [Database server](#database-server)
  - [Database setup for the producer](#database-setup-for-the-producer)
  - [Database setup for the consumer](#database-setup-for-the-consumer)
  - [Implementing the transactional outbox producer](#implementing-the-transactional-outbox-producer)
  - [Implementing the transactional inbox consumer](#implementing-the-transactional-inbox-consumer)
  - [Message format](#message-format)
- [Strategies](#strategies)
  - [Concurrency strategy](#concurrency-strategy)
  - [Message retries strategy](#message-retries-strategy)
  - [Poisonous message retries strategy](#poisonous-message-retries-strategy)
  - [Message processing timeouts strategy](#message-processing-timeouts-strategy)
  - [Message processing Transaction level strategy](#message-processing-transaction-level-strategy)
- [Testing](#testing)

# Context

A service often needs to update data in its database and send events (when its
data was changed) and commands (to request some action in another service). All
the database operations can be done in a single transaction. But sending the
message must (most often) be done as a separate action. This can lead to data
inconsistencies if one or the other fails.

Storing a product order in the Order Service but failing to send an event to the
shipping service can lead to products not being shipped - case 1 in the diagram
below. This would be the case when the database transaction succeeded but
sending the event failed. Sending the event first and only then completing the
database transaction does not fix it either. The products might then be shipped
but the system would not know the order for which the shipment was done (case
2).

Similar issues exist in the Shipment Service. The service might receive a
message and start the shipment but fail to acknowledge the message. The message
would then be re-sent and if the operation is not done in an idempotent way
there might be two shipments for a single order (case 3). Or the message is
acknowledged as handled but submitting the database transaction fails. Then the
message is lost and the customer will not receive the goods (case 4).

![postgresql_outbox_pattern](https://user-images.githubusercontent.com/9946441/286744174-d8df1317-ce78-4be3-94a1-c48f9bc57331.png)
_Potential messaging pitfalls_

One way to solve this would be to use distributed transactions using a two-phase
commit (2PC). PostgreSQL supports the 2PC but many message brokers and event
streaming solutions like RabbitMQ and Kafka do not support it. And even if they
do, it would often not be good to use 2PC. Distributed transactions need to lock
data until all participating services commit their second phase. This works
often well for a small number of services. But with a lot of (micro) services
communicating with each other this often leads to severe bottlenecks.

# The Patterns

When delivering messages we mostly have three different delivery guarantees:

- **At most once** - which is the most basic variant. It tries to send the
  message but if it fails the message is lost.
- **At least once** - this is the case when we guarantee that the message will
  be delivered. But we make no guarantee on how often we send the message. At
  least once - but potentially multiple times.
- **Exactly once** - this is on the delivery itself most often considered to not
  be possible. Multiple mechanisms need to play together to ensure that a
  message is sent (at least) once but guaranteed to be processed (exactly) once.

The transactional outbox and transactional inbox pattern provide such a
combination that guarantees exactly-once **processing** (with at least once
delivery).

You might want to use the transactional inbox pattern in scenarios where the
reliability and atomicity of message processing are important, such as in an
event-driven architecture where the loss or duplication of a message could have
significant consequences.

This diagram shows the involved components to implement both the transactional
outbox and the transactional inbox pattern. The following chapters explain the
components and the interactions in detail.

![postgresql_outbox_pattern](https://user-images.githubusercontent.com/9946441/286744183-7b616f46-e4d8-4d6f-88c1-6ceae5207997.png)
_Components involved in the transactional outbox and inbox pattern
implementation_

## What is the transactional outbox pattern

The transactional outbox pattern is a design pattern that allows you to reliably
"send" messages within the context of a database transaction. It is used to
ensure that messages are only sent if the transaction is committed and that they
are not lost in case of failure.

The transactional outbox pattern uses the following approach to guarantee the
exactly-once processing (in combination with the transactional inbox pattern):

1. Some business logic needs to update data in its database and send an event
   with details of the changes.
2. It uses a database transaction to add all of the business data changes on the
   one hand and it inserts the details of the message that should be sent into
   an "outbox" table within the same database transaction. The message includes
   the necessary information to send the message to the intended recipient(s),
   such as the message payload and the destination topic or queue. This ensures
   that either everything is persisted in the database or nothing. The
   `outbox storage` component in the above diagram encapsulates the logic to
   store messages in the outbox table.
3. A background process notices when a new entry appears in the "outbox" table
   and loads the message data.
4. Now the message can be sent. This can be done via a message broker, an event
   stream, or any other option. The outbox table entry is then marked as
   processed _only_ if the message was successfully sent. In case of a
   message-sending error, or if the outbox entry cannot be marked as processed,
   the message is sent again. The `outbox listener` is responsible for this
   step.

The third step can be implemented via the Polling-Publisher pattern or the
Transactional Log Tailing pattern.

- **Polling-Publisher**: an external process queries the outbox table on a
  (short) interval. This has the drawback, that the database is put under load
  and often results in no newly found entries.
- **Transactional Log Tailing** (also called Capture-Based Log Tailing) reads
  from the transactional log or change stream that contains the changes to the
  outbox table. For PostgreSQL, this is handled with the Write-Ahead Log (WAL)
  and logical replication which is depicted in the diagram and described further
  down in more detail. Using this approach, the transactional outbox pattern can
  be implemented with minimal impact on the database, as the WAL tailing process
  does not need to hold locks or block other transactions.

You can read more about the transactional outbox pattern in this
[microservices.io](https://microservices.io/patterns/data/transactional-outbox.html)
article.

## What is the transactional inbox pattern

The _transactional inbox_ pattern targets the message consumer side of the
process. It is a design pattern that allows you to reliably receive messages and
process each message exactly once.

The transactional inbox pattern uses the following approach to guarantee the
exactly-once processing (in combination with the transactional outbox pattern):

1. A message is sent to a message broker (such as RabbitMQ) and stored in a
   queue, event stream, or any other location.
2. A background process consumes messages from the queue and inserts them into
   an "inbox" table in the database. The process uses the unique message
   identifier to store the message in the inbox as the primary key. This ensures
   that each message is only written once to this database table even if the
   same message was delivered multiple times. The `inbox storage` component from
   the above diagram handles this step.
3. A consumer process receives the messages that were stored in the inbox table
   and processes them. It uses a transaction to store all the service-relevant
   data and mark the inbox message in the inbox table as processed. If an error
   happens during message processing the message can be retried for a
   configurable amount of attempts. This is done by the `inbox listener` in the
   above diagram.

Step three can be implemented again as a Polling-Publisher or via the
Transactional Log Tailing approach like for the outbox pattern.

## What is the PostgreSQL Logical Replication

PostgreSQL logical replication is a method that offers the possibility of
replicating data from one PostgreSQL server to another PostgreSQL server or
other consumer. It works by streaming the changes that are made to the data in a
database in a logical, row-based format, rather than replicating at the physical
storage level.

This is achieved by using a feature called "Logical Replication Slot", which
allows the primary PostgreSQL server (publisher) to stream changes made to a
specific table or set of tables to a replication client. The client can then
apply these changes to its own database (effectively replicating the data). Or
more generally the client can use those changes for any kind of
updates/notifications.

The replication process begins with the creation of a replication slot on the
primary database server. A replication slot is a feature on the PostgreSQL
server that persists information about the state of replication streams.
Replication slots serve to retain WAL (Write-Ahead Logging) files on the
publisher, ensuring that the required logs for replication are not removed
before the subscribing server received them. It keeps track of the last WAL
location that was successfully sent to the subscriber, so that upon reconnection
after a disconnect, replication can resume from that position without missing
any data. In the transactional outbox/inbox pattern case, the outbox and inbox
tables are replicated.

A publisher prepares and sends the stream of data changes from specified tables
to the subscribers. For the transactional outbox and inbox scenario the outbox
and inbox tables are configured for publication. The publisher creates a set of
changes that need to be replicated based on inserts, updates, and deletes on the
published tables. These changes are sent in the form of "WAL records"
(Write-Ahead Log records). Publications are used in conjunction with
subscriptions to set up logical replication from the publisher to the
subscriber.

When the subscriber (client) connects to the publisher, it specifies the name of
the logical replication slot it wants to use. The publisher uses this
information to start streaming changes from the point in the WAL that is stored
in the replication slot. As the subscriber receives changes, it updates the
position of the replication slot on the publisher to keep track of where it is
in the stream. The position defines the last data record that the client
successfully consumed and acknowledged. It is not possible to acknowledge only
specific WAL records - everything up to this position is acknowledged.

In this way, the publisher and subscriber can maintain a consistent position in
the replication stream, allowing the subscriber to catch up with any changes
that may have occurred while it was disconnected.

# The pg-transactional-outbox library overview

This library implements the transactional outbox and inbox pattern using the
PostgreSQL server and the Transactional Log Tailing approach via the PostgreSQL
Write-Ahead Log (WAL).

You can use the library in your node.js projects that use a PostgreSQL database
as the data store and some message sending/streaming software (e.g. RabbitMQ or
Kafka).

## Database server

The PostgreSQL database server itself must have the `wal_level` configured as
`logical`. This enables the use of the WAL to be notified on e.g. new inserts.
You should also check the `max_wal_senders`, the `max_wal_size`, and the
`min_wal_size` settings to contain values that match your architecture. Setting
a large size could consume/max out the disk space. Setting the value too low
could lead to lost events in case your WAL consumer is down for a longer
duration. An example `postgres.conf` file is shown in the `./infra` folder. This
folder includes also a `docker-compose.yml` file to set up a PostgreSQL server
with these default values (and a RabbitMQ instance for running the examples).

This library uses the standard PostgreSQL logical decoding plugin `pgoutput`.
This plugin is included directly in PostgreSQL since version 9.4. Other
alternatives would be wal2json or decoding-json but those are not used in this
library.

## Database setup for the producer

The outbox table in the producer service must have the following structure. You
can decide to put this table in the `public` or some separate database schema
(recommended). You can set this schema in the configuration settings of the
library.

You can follow the steps below or use the
`examples/rabbitmq/producer/setup/init-db.ts` script as an example to generate
the database, roles, and other settings for you.

```sql
CREATE TABLE public.outbox (
  id uuid PRIMARY KEY,
  aggregate_type VARCHAR(255) NOT NULL,
  aggregate_id VARCHAR(255) NOT NULL,
  message_type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

The database role that is used to mutate the business-relevant data must be
granted the select, insert and delete permissions for this outbox table.

```sql
GRANT SELECT, INSERT, DELETE ON public.outbox TO db_login_outbox;
```

Then you need to create a new database role for the WAL subscription. As this
role has a lot of rights it is not advised to give the replication permission to
the same role that reads and mutates the business-relevant data.

The following statements can be used:

```sql
BEGIN;
  CREATE ROLE db_outbox WITH REPLICATION LOGIN PASSWORD 'db_outbox_password';
  CREATE PUBLICATION pg_transactional_outbox_pub
    FOR TABLE public.outbox WITH (publish = 'insert');
COMMIT;

SELECT pg_create_logical_replication_slot('pg_transactional_outbox_slot', 'pgoutput');
```

> **NOTE**: The `BEGIN` and `COMMIT` in the example above are important, because
> the replication slot must be created in a transaction that has not performed
> any writes, otherwise an error would be thrown.

> **NOTE**: the replication slot name is database **server** unique! This means
> if you use the transactional inbox pattern on multiple databases within the
> same PostgreSQL server instance you must use different replication slot names
> for each of them.

## Database setup for the consumer

Corresponding to the outbox table, the consumer service must have the inbox
table. You can also decide to put it in a separate database schema
(recommended).

You can follow again the steps below or use the
`examples/rabbitmq/consumer/setup/init-db.ts` script as an example to generate
the database, roles, and other settings for you.

```sql
CREATE TABLE public.inbox (
  id uuid PRIMARY KEY,
  aggregate_type VARCHAR(255) NOT NULL,
  aggregate_id VARCHAR(255) NOT NULL,
  message_type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL,
  processed_at TIMESTAMPTZ,
  started_attempts smallint NOT NULL DEFAULT 0,
  finished_attempts smallint NOT NULL DEFAULT 0
);
```

The database role that stores the incoming messages in the inbox table must be
granted the select and insert permissions for this table. If the same role is
also used to process the inbox messages later on it requires also the update
permissions for the `started_attempts`, `finished_attempts` and `processed_at`
fields and maybe also the delete permissions to cleanup the inbox from old
messages.

```sql
GRANT SELECT, INSERT ON public.inbox TO db_login_inbox;
GRANT UPDATE (started_attempts, finished_attempts, processed_at) ON public.inbox TO db_login_inbox;
```

Just like for accessing the outbox table WAL, the inbox solution requires a
similar setup:

```sql
BEGIN;
  CREATE ROLE db_inbox WITH REPLICATION LOGIN PASSWORD 'db_inbox_password';
  CREATE PUBLICATION pg_transactional_inbox_pub
  FOR TABLE public.inbox WITH (publish = 'insert');
COMMIT;
SELECT pg_create_logical_replication_slot('pg_transactional_inbox_slot', 'pgoutput');
```

## Implementing the transactional outbox producer

The following code shows the producer side of the transactional outbox pattern.
The two main functions are the `initializeOutboxListener` to listen to the WAL
messages when an outbox message is written to the outbox table. And the
`initializeOutboxMessageStorage` generator function to store outgoing messages
in the outbox table (for a specific aggregate type and message type).

```TypeScript
import process from 'node:process';
import { Client } from 'pg';
import {
  OutboxConfig,
  OutboxMessage,
  createMutexConcurrencyController,
  getDefaultLogger,
  initializeOutboxListener,
  initializeOutboxMessageStorage,
} from 'pg-transactional-outbox';

(async () => {
  const logger = getDefaultLogger('outbox');

  // Initialize the actual message publisher e.g. publish the message via RabbitMQ
  const messagePublisher = async (message: OutboxMessage): Promise<void> => {
    // In the simplest case the message can be sent via inter process communication
    process.send?.(message);
  };

  const config: OutboxConfig = {
    pgReplicationConfig: {
      host: 'localhost',
      port: 5432,
      user: 'db_outbox',
      password: 'db_outbox_password',
      database: 'pg_transactional_outbox',
    },
    settings: {
      dbSchema: 'public',
      dbTable: 'outbox',
      postgresPub: 'pg_transactional_outbox_pub',
      postgresSlot: 'pg_transactional_outbox_slot',
    },
  };

  // Initialize and start the outbox subscription. This listener receives all the
  // outbox table inserts from the WAL. It executes the messagePublisher function
  // with every received outbox message. It cares for the at least once delivery.
  const [shutdown] = initializeOutboxListener(
    config,
    messagePublisher,
    logger,
    {
      concurrencyStrategy: createMutexConcurrencyController(),
      messageProcessingTimeoutStrategy: (message: OutboxMessage) =>
        message.messageType === 'ABC' ? 10_000 : 2_000,
    },
  );

  // Initialize the outbox storage function. This function encapsulates the
  // aggregate type (movie) and the message type (movie_created). It will be
  // called to insert the outgoing message into the outbox as part of the DB
  // transaction that is responsible for this event.
  // You can alternatively use initializeGeneralOutboxMessageStorage for a
  // outbox message storage function to store different message/aggregate types.
  const storeOutboxMessage = initializeOutboxMessageStorage(
    'movie',
    'movie_created',
    config,
  );

  // The actual business logic generates in this example a new movie in the DB
  // and wants to reliably send a "movie_created" message.
  const client = new Client({
    host: 'localhost',
    port: 5432,
    user: 'db_login_outbox',
    password: 'db_login_outbox_password',
    database: 'pg_transactional_outbox',
  });

  try {
    client.connect();
    // The movie and the outbox message must be inserted in the same transaction.
    await client.query('START TRANSACTION ISOLATION LEVEL SERIALIZABLE');
    // Insert the movie (and query/mutate potentially a lot more data)
    const result = await client.query(
      `INSERT INTO public.movies (title) VALUES ('some movie') RETURNING id, title;`,
    );
    // Store the message in the outbox table
    await storeOutboxMessage(result.rows[0].id, result.rows[0], client);
    // (Try to) commit the transaction to save the movie and the outbox message
    await client.query('COMMIT');
    client.end();
  } catch (err) {
    // In case of an error roll back the DB transaction - neither movie nor
    // the outbox message will be stored in the DB now.
    await client?.query('ROLLBACK');
    client.end();
    throw err;
  }
  await shutdown();
})();
```

### Outbox Storage

The outbox storage is used to store the message that should later be sent into
the `outbox` database table. It comes in two flavors:

The `initializeOutboxMessageStorage` is initialized to store a specific
combination of aggregate type and message type. In the above example, it is used
to store outbox messages for the aggregate type `movie` and the message type
`movie_created`. The storage function accepts the following parameters:

- `aggregateId`: the unique identifier of the aggregate - e.g. the movie
  database ID
- `payload`: the message payload that is used on the receiving side to act on
  such a message.
- `dbClient`: the database client with an open database transaction to both do
  the business logic as well as store the outgoing message within that
  transaction.
- `metadata`: optional metadata that can be used for the actual message
  transport like routing keys and the target queue.

The more generic outbox storage can be created with the
`initializeGeneralOutboxMessageStorage` function. It has the same input
parameters as the above storage function but in addition also the
`aggregateType` and the `messageType`.

### Outbox Listener

The `initializeOutboxListener` is used to create and start the WAL listener that
gets notified when a new outbox message is written to the outbox table. It takes
the message publisher instance as an input parameter (`sendMessage`) and calls
it to send out messages. Other parameters are the `config` object, a `logger`
instance and an optional strategies object to fine-tune and customize specific
aspects of the outbox listener.

### Message Publisher

The message publisher is responsible for the actual transport of the message to
the recipient(s). It receives the outbox `message` with all the fields -
including the metadata. Based on the metadata the message publisher should send
out the message. The messaging publisher logic or the used system is responsible
for guaranteeing at least once message delivery. You can find a RabbitMQ-based
implementation in the examples folder.

## Implementing the transactional inbox consumer

A minimalistic example code for the consumer of the published message using the
transactional inbox pattern. The main functions are the
`initializeInboxMessageStorage` function that is used by the actual message
receiver like a RabbitMQ-based message handler to store the incoming message
(which was based on an outbox message) in the inbox table. The other central
function is `initializeInboxListener`. It uses one database connection based on
a user with replication permission to receive notifications when a new inbox
message is created. It uses a second database connection to open a transaction,
load the inbox message from the database and lock it, execute the message
handler queries/mutations, and finally mark the inbox message as processed in
the database.

```TypeScript
import { ClientBase } from 'pg';
import {
  InboxConfig,
  InboxMessage,
  IsolationLevel,
  OutboxMessage,
  createMultiConcurrencyController,
  getDefaultLogger,
  initializeInboxListener,
  initializeInboxMessageStorage,
} from 'pg-transactional-outbox';

/** The main entry point of the message producer. */
(async () => {
  const logger = getDefaultLogger('outbox');

  // Configuration settings for the replication and inbox table configurations
  const config: InboxConfig = {
    // This configuration is used to start a transaction that locks and updates
    // the row in the inbox table that was found from the WAL log. This connection
    // will also be used in the message handler so every select and data change is
    // part of the same database transaction. The inbox database row is then
    // marked as "processed" when everything went fine.
    pgConfig: {
      host: 'localhost',
      port: 5432,
      user: 'db_login_inbox',
      password: 'db_login_inbox_password',
      database: 'pg_transactional_inbox',
    },
    // Configure the replication role to receive notifications when a new inbox
    // row was added to the inbox table. This role must have the replication
    // permission.
    pgReplicationConfig: {
      host: 'localhost',
      port: 5432,
      user: 'db_inbox',
      password: 'db_inbox_password',
      database: 'pg_transactional_inbox',
    },
    settings: {
      dbSchema: 'public',
      dbTable: 'outbox',
      postgresPub: 'pg_transactional_inbox_pub',
      postgresSlot: 'pg_transactional_outbox_slot',
    },
  };

  // Initialize the inbox message storage to store incoming messages in the inbox
  const [storeInboxMessage, shutdownInboxStorage] =
    initializeInboxMessageStorage(config, logger);

  // Initialize the message receiver e.g. based on RabbitMQ
  // In the simplest scenario use the inter process communication:
  process.on('message', async (message: OutboxMessage) => {
    await storeInboxMessage(message);
  });

  // Define an optional concurrency strategy to handle messages with the message
  // type "ABC" in parallel while handling other messages sequentially per
  // aggregate type and message type combination.
  const concurrencyStrategy = createMultiConcurrencyController(
    (message) => {
      switch (message.messageType) {
        case 'ABC':
          return 'full-concurrency';
        default:
          return 'discriminating-mutex';
      }
    },
    (message) => `${message.aggregateType}.${message.messageType}`,
  );

  // Initialize and start the inbox subscription
  initializeInboxListener(
    config,
    // This array holds a list of all message handlers for all the aggregate
    // and message types.
    [
      {
        aggregateType: 'movie',
        messageType: 'movie_created',
        handle: async (
          message: InboxMessage,
          client: ClientBase,
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
          message: InboxMessage,
          _client: ClientBase,
          { current, max }: { current: number; max: number },
        ): Promise<'transient_error' | 'permanent_error'> => {
          // Lets you decide if a message should be retried (transient error) or
          // not (permanent error). You can run also run your compensation code.
          if (current < max) {
            return 'transient_error';
          }
          logger.error(
            error,
            `Giving up processing message with ID ${message.id}.`,
          );
          return 'permanent_error';
        },
      },
    ],
    logger,
    {
      concurrencyStrategy,
      messageProcessingTimeoutStrategy: (message: OutboxMessage) =>
        message.messageType === 'ABC' ? 10_000 : 2_000,
      messageProcessingTransactionLevel: (message: OutboxMessage) =>
        message.messageType === 'ABC'
          ? IsolationLevel.ReadCommitted
          : IsolationLevel.RepeatableRead,
    },
  );
  await shutdownInboxStorage();
})();

```

> **Please note:** This library does not automatically delete inbox messages
> from the inbox table. Keeping them is required to ensure that a message is
> only processed once (due to multiple deliveries or replay attacks). Please
> define your own logic on how long the messages should stay in this table. This
> can be a few hours but also some days. Messages that are older than this
> defined duration should not be processed anymore.

### Message receiver

The message receiver is the counterpart to the message publisher. It receives
the transferred message and has to store it in the inbox table. It is the
responsibility of the receiver to guarantee that the message is written to the
inbox table via the inbox storage functionality. It can try it multiple times if
needed. You can find a RabbitMQ-based implementation in the examples folder.

### Message Storage

When a message is received it must use the inbox storage functionality to store
the message. The `initializeInboxMessageStorage` is used to create to create an
inbox storage instance. The inbox storage takes the incoming `OutboxMessage` and
stores it as an `InboxMessage` in the inbox table.

### Inbox Listener

The `initializeInboxListener` initializes the inbox listener which is notified
whenever a new inbox message is written to the inbox table. It takes a list of
message handlers and invokes the one that matches the incoming messages
aggregate type and message type.

The optional `strategies` object can be provided to customize different inbox
listener scenarios.

### Message Handler

The message handler is a component that defines how to process messages of a
specific aggregate type and message type. The message handler is used by the
transactional inbox service to process messages in a reliable and consistent
way. The service will invoke the `handle` function of the appropriate handler
for each message in the inbox, and handle any errors or retries using the
`handleError` function if provided.

It implements the `InboxMessageHandler` interface, which has the following
properties:

- `aggregateType`: The name of the aggregate root type that the message belongs
  to. For example, `movie`, `customer`, `product`, etc.
- `messageType`: The name of the message type that the handler can handle. For
  example, `movie_created`, `customer_updated`, `restock_product`, etc.
- `handle`: A function that contains the custom business logic to handle an
  inbox message. It receives two parameters: `message` and `client`. The
  `message` parameter is an object that contains the message id, payload, and
  metadata. The `client` parameter is a database client that is part of a
  transaction to safely handle the inbox message. The function should return a
  promise that resolves when the message is successfully processed, or rejects
  with an error if the message cannot be processed. The error will cause the
  message to be retried later.
- `handleError`: An optional function that contains the custom business logic to
  handle an error that was caused by the `handle` function. It receives four
  parameters: `error`, `message`, `client`, and `attempts`. The `error`
  parameter is the error that was thrown in the `handle` function. The `message`
  parameter is the same as in the `handle` function. The `client` parameter is a
  database client that is part of a new transaction to safely handle the error.
  The `attempts` parameter is an object that contains the current and maximum
  number of times the message will be attempted. The function should return a
  promise that resolves to a flag that defines if the message should be retried
  (`transient_error`) or not (`permanent_error`).

## Message format

Messages are the means to transport information in a structured way from the
message producer to the message consumer.

Both the outbox and inbox message have the following properties:

| Field Name    | Field Type | Description                                                                            |
| ------------- | ---------- | -------------------------------------------------------------------------------------- |
| id            | string     | The unique identifier of the message. Used to ensure a message is only processed once. |
| aggregateType | string     | The type of the aggregate root (in DDD context) to which this message is related.      |
| aggregateId   | string     | The unique identifier of the aggregate.                                                |
| messageType   | string     | The type name of the event or command.                                                 |
| payload       | object     | The message payload with the details for an event or instructions for a command.       |
| metadata      | object     | Optional metadata used for the actual message transfer.                                |
| createdAt     | string     | The date and time in ISO 8601 UTC format when the message was created.                 |

In addition, the inbox message keeps track of when it was processed and the
number of attempts it took/currently has done.

| Field Name        | Field Type | Description                                                                               |
| ----------------- | ---------- | ----------------------------------------------------------------------------------------- |
| started_attempts  | number     | The number of times an inbox message was attempted to be processed.                       |
| finished_attempts | number     | The number of times an inbox message was processed (successfully or with a caught error). |
| processedAt       | string     | The date and time in ISO 8601 UTC format when the message was processed.                  |

# Strategies

The strategy pattern is a behavioral design pattern that enables selecting an
algorithm at runtime. Instead of implementing a single algorithm directly, the
listeners receive run-time instructions as to which algorithm should be used.
This allows you to customize different parts of the message handling for
concurrency, retries, poisonous message handling, etc. By defining a common
interface for different scenarios you can use either existing code or write your
custom implementations.

## Concurrency strategy

The outbox and inbox listeners process messages that are stored in their
corresponding tables. When they process the messages, you can influence the
level of concurrency of the listeners. The default concurrency controller will
use a mutex to guarantee sequential message processing. There are the following
pre-build ones but you can also write your own:

- `createFullConcurrencyController` - this controller allows the parallel
  processing of messages without guarantees on the processing order.
- `createMutexConcurrencyController` - this controller guarantees sequential
  message processing across all messages.
- `createSemaphoreConcurrencyController` - this controller allows the processing
  of messages in parallel up to a configurable number.
- `createDiscriminatingMutexConcurrencyController` - this controller enables
  sequential message processing based on a specified discriminator. This could
  be the message type or some other (calculated) value. The controller still
  guarantees sequential message processing but only across messages with the
  same discriminator.
- `createMultiConcurrencyController` - this is a combined concurrency
  controller. You can define which of the above controllers should be used for
  different kinds of messages.

## Message processing client strategy

Inbox messages are processed in a database transaction that verifies from the
inbox table if the message was not processed already and loads the
retry-specific data. It handles also the business-logic related database work.

Some message handlers may require a database login with elevated permissions
while others can use more restricted users. With this strategy, you can return a
database client from your desired Pool in the `getClient` function. When the
inbox listener is shut down it will call the `shutdown` function where you can
close your used database pools and run other cleanup logic.

## Message processing timeout strategy

The `messageProcessingTimeoutStrategy` allows you to define a message-based
timeout on how long the message is allowed to be processed (in milliseconds).
This allows you to give more time to process "expensive" messages while still
processing others on a short timeout. By default, it uses the configured
`messageProcessingTimeout` or falls back to a 15-second timeout.

## Message processing Transaction level strategy

The inbox listener lets you define the `messageProcessingTransactionLevel` per
message. Some message processing may have higher isolation level requirements
than others. If no custom strategy is provided it uses the default database
transaction level via `BEGIN`.

## Message retry strategy

When processing an inbox message an error can be thrown. The inbox listener
catches that error and needs to decide if the message should be processed
again - or not. The `messageRetryStrategy` offers the possibility to customize
the decision if a message should be retried or not. By default, the
`defaultMessageRetryStrategy` is used. It will retry the message up the
configured value in the `maxAttempts` setting or as a fallback five times
(including the initial attempt).

## Poisonous message retry strategy

A poisonous message is a message that causes the service to crash repeatedly and
prevents other messages from being processed. To avoid this situation, the
service tracks the number of times a message is started and finished, regardless
of the outcome (success or error). If the service crashes while processing a
message, it will retry the message until it succeeds or reaches a maximum number
of attempts.

You can customize the behavior of the service by changing the following options:

- `settings.maxPoisonousAttempts`: This is the maximum number of times the
  service will retry a message before marking it as poisonous. The default value
  is 3, but you can change it to any positive integer.
- `poisonousMessageRetryStrategy`: This is a function that determines whether a
  message should be retried or not, based on the started and finished counts.
  The default function is (started, finished) => started - finished >= 3, but
  you can implement your own logic and pass it as an argument to the service
  constructor.

## Listener restart time strategy

When the inbox or outbox listener fails due to an error it is restarted. The
`listenerRestartTimeStrategy` is used to define how long it should wait before
it attempts to start again based on the caught error.

The `defaultListenerRestartTimeStrategy` checks if the error message is a
PostgreSQL error telling that the replication slot is in use. If this is the
case it will wait for the configured `restartDelaySlotInUse` (default: 10sec)
time and otherwise for the configured `restartDelay` (default: 250ms) one.

# Testing

The `__tests__` folder contains integration tests that test the functionality of
the outbox and inbox listener implementation. The tests are using the
`testcontainers` library to start up a new docker PostgreSQL server.

You can simply run the `test` script to execute the tests.

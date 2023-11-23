# pg-transactional-outbox

The `pg-transactional-outbox` is a library that implements the transactional
outbox and transactional inbox pattern for PostgreSQL. It allows you to reliably
send and receive messages within the context of a PostgreSQL transaction,
ensuring that messages are only sent or received if the transaction is
committed.

![postgresql_outbox_pattern](https://user-images.githubusercontent.com/9946441/211221740-a10d3c0b-dfa9-4c4e-84fb-068f6e63aaac.jpeg)
_Message delivery via the PostgreSQL-based transactional outbox pattern
(generated with Midjourney)_

**Table of contents:**

- [Context](#context)
- [Patterns](#patterns)
  - [What is the transactional outbox pattern](#what-is-the-transactional-outbox-pattern)
  - [What is the transactional inbox pattern](#what-is-the-transactional-inbox-pattern)
  - [What is the PostgreSQL Logical Replication](#what-is-the-postgresql-logical-replication)
- [The pg-transactional-outbox library](#the-pg-transactional-outbox-library)
  - [Database server](#database-server)
  - [Database setup for the producer](#database-setup-for-the-producer)
  - [Database setup for the consumer](#database-setup-for-the-consumer)
  - [Implementing the transactional outbox producer](#implementing-the-transactional-outbox-producer)
  - [Implementing the transactional inbox consumer](#implementing-the-transactional-inbox-consumer)
- [Testing](#testing)

# Context

A service often needs to update data in its database and send events (when its
data was changed) and commands (to request some action in another service). All
the database operations can be done in a single transaction. But sending the
message must (most often) be done as a different action. This can lead to data
inconsistencies if one or the other fails.

Storing a product order in the order service but failing to send an event to the
shipping service can lead to products not being shipped. This would be the case
when the database transaction succeeded but sending the event failed. Sending
the event first and only then completing the database transaction does not fix
it either. The products might then be shipped but the system would not know the
order for which the shipment was done.

One way to solve this would be to use distributed transactions using a two-phase
commit (2PC). PostgreSQL supports the 2PC but many message brokers and event
streaming solutions like RabbitMQ and Kafka do not support it. And even if they
do, it would often not be good to use 2PC. Distributed transactions need to lock
data until all participating services commit their second phase. This works
often well for a small number of services. But with a lot of (micro) services
communicating with each other this often leads to severe bottlenecks.

When delivering messages we mostly have three different delivery guarantees:

- **At most once** - which is the most basic variant. It tries to send the
  message but if it fails the message is lost.
- **At least once** - this is the case when we guarantee that the message will
  be delivered. But we make no guarantee on how often we send the message. At
  least once - but potentially multiple times.
- **Exactly once** - this is on the delivery itself most often considered to not
  be possible. Multiple mechanisms need to play together to ensure that a
  message is sent (at least) once but guaranteed to be processed only once.

The transactional outbox and transactional inbox pattern provide such a
combination that guarantees exactly-once **processing** (with at least once
delivery).

You might want to use the transactional inbox pattern in scenarios where the
reliability and atomicity of message processing are important, such as in an
event-driven architecture where the loss or duplication of a message could have
significant consequences.

# Patterns

![postgresql_outbox_pattern](https://user-images.githubusercontent.com/9946441/215294397-b9b622a1-f923-4d28-a3e0-2ca9f849b63b.png)
_Components involved in the transactional outbox and inbox pattern
implementation_

## What is the transactional outbox pattern

The transactional outbox pattern is a design pattern that allows you to reliably
send messages within the context of a database transaction. It is used to ensure
that messages are only sent if the transaction is committed and that they are
not lost in case of failure.

The transactional outbox pattern uses the following approach to guarantee the
exactly-once processing (in combination with the transactional inbox pattern):

1. Some business logic needs to update data in its database and send an event
   with details of the changes.
2. It uses a database transaction to add all of its mutations and inserts the
   details of the message it needs to send into an "outbox" table within the
   same database transactions. The message includes the necessary information to
   send the message to the intended recipient(s), such as the message payload
   and the destination topic or queue. This ensures that either everything is
   persisted in the database or nothing. This is the `outbox storage` in the
   above diagram.
3. A background process checks if a new entry appears in the "outbox" table and
   loads the data.
4. Now the message can be sent. This can be done via a message broker, an event
   stream, or any other option. The outbox table entry is then marked as
   processed _only_ if the message was successfully sent. In case of a
   message-sending error, or if the outbox entry cannot be marked as processed,
   the message is sent again. The `outbox service` is responsible for this step.

The third step can be implemented via the Polling-Publisher pattern or via the
Transactional log tailing pattern.

- Polling-Publisher: an external process queries the outbox table on a (short)
  interval. This has the drawback, that the database is put under load and often
  results in no newly found entries.
- Transactional (also called capture-based) log tailing reads from the
  transactional log or change stream that contains the changes to the outbox
  table. For PostgreSQL, this is handled with the Write-Ahead Log (WAL) and
  logical replication which is described further down in more detail. Using this
  approach, the transactional outbox pattern can be implemented with minimal
  impact on the database, as the WAL tailing process does not need to hold locks
  or block other transactions.

You can read more about the transactional outbox pattern in this
[microservices.io](https://microservices.io/patterns/data/transactional-outbox.html)
article.

## What is the transactional inbox pattern

The transactional outbox pattern solves the challenges from the producing side.

The _transactional inbox_ pattern targets the message consumer. It is a design
pattern that allows you to reliably receive and process messages within the
context of a database transaction. It is used to ensure that messages are only
processed if the transaction is committed and that they are not lost or
processed multiple times in case of failure.

The transactional inbox pattern uses the following approach to guarantee the
exactly-once processing (in combination with the transactional outbox pattern):

1. A message was sent to a message broker (such as RabbitMQ) and stored in a
   queue. Or it was sent to an event stream or another message exchange.
2. A background process consumes messages from the queue and inserts them into
   an "inbox" table in the database. The process uses the unique message
   identifier to store the message in the inbox as the primary key. This ensures
   that each message is only written once to this database table. The
   `inbox storage` component from the above diagram handles this step.
3. A consumer process receives the messages that were stored in the inbox table
   and processes them. It uses a transaction to store service relevant data and
   mark the inbox message as processed. This is done by the `inbox service` in
   the above diagram.

Step three can be implemented again as a Polling-Publisher or via the
Transactional log tailing approach like for the outbox pattern.

An added benefit of the transactional inbox is, that the message processing is
now decoupled from the message receiving. In many message brokers, a message is
sent to the consumer and locked for other consumers. Once the consumer finishes
the message processing it sends an acknowledgment that the message was
successfully processed and can be removed from e.g. the queue. If the time until
the message is acknowledged takes too long, the message could be made available
again. But at least it will keep a lock on the message for a longer time. With
the inbox pattern, the message is not processed when it is received but only
stored in the inbox table. The actual processing can then take whatever time is
needed.

## What is the PostgreSQL Logical Replication

PostgreSQL logical replication is a method that offers the possibility of
replicating data from a PostgreSQL database to another PostgreSQL database or
other consumer. It works by streaming the changes that are made to the data in a
database in a logical, row-based format, rather than replicating at the physical
storage level.

This is achieved by using a feature called "Logical Replication Slot", which
allows a PostgreSQL server to stream changes made to a specific table or set of
tables to a replication client. The client can then apply these changes to its
own database (effectively replicating the data). Or more generally the client
can use those changes for any kind of updates/notifications.

The replication process begins with the creation of a replication slot on the
primary database server. This is a named data structure that holds information
about the replication process, such as the current position of the replication
stream and the name of the table(s) that are being replicated. In the
transactional outbox/inbox pattern case, the outbox and inbox table are
replicated.

Once the replication slot is created, the primary server will begin streaming
changes made to the specified table(s) to the replication client. These changes
are sent in the form of "WAL records" (Write-Ahead Log records), which are the
individual changes made to the data in the inbox/outbox tables.

The publisher keeps track of the current position of the replication stream
using the logical replication slot. A logical replication slot is a named data
structure on the publisher that stores information about the current position of
the replication stream, including the transaction ID and the write-ahead log
(WAL) location.

When the subscriber (client) connects to the publisher, it specifies the name of
the logical replication slot it wants to use. The publisher uses this
information to start streaming changes from the point in the WAL that is stored
in the replication slot. As the subscriber receives changes, it updates the
position of the replication slot on the publisher to keep track of where it is
in the stream. The position defines the last data record that the client
successfully consumed and acknowledged. It is not possible to acknowledge only
specific messages - everything up to this position is acknowledged.

In this way, the publisher and subscriber can maintain a consistent position in
the replication stream, allowing the subscriber to catch up with any changes
that may have occurred while it was disconnected.

# The pg-transactional-outbox library

This library implements the transactional outbox and inbox pattern using the
PostgreSQL server and the Transactional log tailing approach via the PostgreSQL
Write-Ahead Log (WAL).

You can use the library in your node.js projects that use a PostgreSQL database
as the data store and some event-sending approach (e.g. RabbitMQ or Kafka).

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

This library uses the standard PostgreSQL logical decoding plugin `pgoutput`
plugin. This plugin is included directly in PostgreSQL since version 9.4. Other
alternatives would be wal2json, decoding-json, and others (not used in this
library).

## Database setup for the producer

The outbox table in the producer service must have the following structure. You
can decide to put this table in the `public` or some separate database schema.
You can set this schema in the configuration settings of the library.

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
granted the select, insert and delete permissions for this table.

```sql
GRANT SELECT, INSERT, DELETE ON public.outbox TO db_login_outbox;
```

Then you need to create a new database role for the WAL subscription. As this
role has a lot of rights it is not advised to give this role to the same role
that reads and mutates the business-relevant data.

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
table. You can also decide to put it in a separate database schema.

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
  attempts smallint NOT NULL DEFAULT 0
);
```

The database role that reads from this table and should mutate the
business-relevant data must be granted the select, insert and delete permissions
for this table.

```sql
GRANT SELECT, INSERT, DELETE ON public.inbox TO db_login_inbox;
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
The two main functions are the `initializeOutboxService` to listen to the WAL
messages when an outbox message was written to the outbox table. And the
`initializeOutboxMessageStorage` generator function to store outgoing messages
in the outbox table (for a specific aggregate type and message type).

```TypeScript
import process from 'node:process';
import { Client } from 'pg';
import {
  createMutexConcurrencyController,
  getDefaultLogger,
  initializeOutboxMessageStorage,
  initializeOutboxService,
  OutboxMessage,
  OutboxServiceConfig,
} from 'pg-transactional-outbox';

(async () => {
  const logger = getDefaultLogger('outbox');

  // Initialize the actual message publisher e.g. publish the message via RabbitMQ
  const messagePublisher = async (message: OutboxMessage): Promise<void> => {
    // In the simplest case the message can be sent via inter process communication
    process.send?.(message);
  };

  const config: OutboxServiceConfig = {
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
  const concurrencyController = createMutexConcurrencyController();

  // Initialize and start the outbox subscription. This service receives all the
  // outbox table inserts from the WAL. It executes the messagePublisher function
  // with every received outbox message. It cares for the at least once delivery.
  const [shutdown] = initializeOutboxService(
    config,
    messagePublisher,
    logger,
    concurrencyController,
  );

  // Initialize the outbox storage function. This function encapsulates the
  // aggregate type (movie) and the message type (movie_created). It will be
  // called to insert the outgoing message into the outbox as part of the DB
  // transaction that is responsible for this event.
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
    await client.query('BEGIN');
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

## Implementing the transactional inbox consumer

A minimalistic example code for the consumer of the published message using the
transactional inbox pattern. The main functions are the
`initializeInboxMessageStorage` function that is used by the actual message
receiver like a RabbitMQ-based message handler to store the incoming message
(which was based on an outbox message) in the inbox table. The other central
function is `initializeInboxService`. It uses one database connection based on a
user with the replication permission to receive notifications when a new inbox
message was created. And a second database connection to open a transaction,
load the inbox message from the database and lock it, execute the message
handler queries/mutations, and finally mark the inbox message as processed in
the database.

```TypeScript
import { ClientBase } from 'pg';
import {
  createMutexConcurrencyController,
  getDefaultLogger,
  InboxMessage,
  InboxServiceConfig,
  initializeInboxMessageStorage,
  initializeInboxService,
  OutboxMessage,
} from 'pg-transactional-outbox';

/** The main entry point of the message producer. */
(async () => {
  const logger = getDefaultLogger('outbox');

  // Configuration settings for the replication and inbox table configurations
  const config: InboxServiceConfig = {
    // This configuration is used to start a transaction that locks and updates
    // the row in the inbox table that was found from the WAL log. This connection
    // will also be used in the message handler so every query and mutation is
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

  const concurrencyController = createMutexConcurrencyController();

  // Initialize and start the inbox subscription
  initializeInboxService(
    config,
    // This array holds a list of all message handlers for all the aggregate
    // and message types. More than one handler can be configured for the same
    // aggregate type and message type.
    [
      {
        aggregateType: 'movie',
        messageType: 'movie_created',
        handle: async (
          message: InboxMessage,
          client: ClientBase,
        ): Promise<void> => {
          // Execute the message handler logic using the same database
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
      },
    ],
    logger,
    concurrencyController,
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

## Testing

The `__tests__` folder contains integration tests that test the functionality of
the outbox and inbox service implementation. The tests are using the
`testcontainers` library to start up a new docker PostgreSQL server.

You can simply run the `test` script to execute the tests.

The script `logical-rep-service` starts a manual test of the used
[LogicalReplicationService](https://github.com/kibae/pg-logical-replication)
library. This can be used to see how the library acts on different outage
scenarios like a lost database server or other issues. This script depends on
the infrastructure that is crated with the `infra:up` script.

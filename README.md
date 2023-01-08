# Library

The `pg-transactional-outbox` is a library that implements the transactional
outbox and transactional inbox pattern for PostgreSQL. It allows you to reliably
send and receive messages within the context of a PostgreSQL transaction,
ensuring that messages are only sent or received if the transaction is
committed.

![postgresql_outbox_pattern](https://user-images.githubusercontent.com/9946441/211221740-a10d3c0b-dfa9-4c4e-84fb-068f6e63aaac.jpeg)
_Message delivery via the PostgreSQL-based transactional outbox pattern
(generated with Midjourney)_

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

# What is the transactional outbox pattern

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
   persisted in the database or nothing.
3. A background process checks if a new entry appeared in the "outbox" table and
   loads the data
4. Now the message is sent. This can be done via a message broker, an event
   stream, or any other option. The outbox table entry is then marked as
   processed _only_ if the message was successfully sent. In case of a
   message-sending error, or if the outbox entry could not be marked as
   processed, the message is sent again.

The third step can be implemented via the Polling-Publisher pattern or via the
Transactional log tailing pattern.

- Polling-Publisher: an external process queries the outbox table on a (short)
  interval. This has the drawback, that the database is put under load and often
  results in no newly found entries.
- Transactional log tailing (also called capture-based) is not querying the
  database itself but reads from the transactional log or change stream. For
  PostgreSQL, this is the Write-Ahead Log (WAL). In PostgreSQL, the WAL is a log
  of changes made to the database that is used to ensure data consistency and
  recoverability in the event of a failure.  
  Using this approach, the transactional outbox pattern can be implemented with
  minimal impact on the database, as the WAL tailing process does not need to
  hold locks or block other transactions.

You can read more about the transactional outbox pattern in this
[microservices.io](https://microservices.io/patterns/data/transactional-outbox.html)
article.

# What is the transactional inbox pattern

The transactional outbox pattern solves the challenges from the producing side.

The _transactional inbox_ pattern targets the message receiver. It is a design
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
   that each message is only written once to this database table.
3. A consumer process reads the messages from the transactional inbox table and
   processes them.

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
  event_type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
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
CREATE ROLE db_outbox WITH REPLICATION LOGIN PASSWORD 'db_outbox_password';
CREATE PUBLICATION pg_transactional_outbox_pub FOR TABLE public.outbox WITH (publish = 'insert');
select pg_create_logical_replication_slot('pg_transactional_outbox_slot', 'pgoutput');
```

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
  event_type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  processed_at TIMESTAMPTZ,
  retries smallint NOT NULL DEFAULT 0
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
CREATE ROLE db_inbox WITH REPLICATION LOGIN PASSWORD 'db_inbox_password';
CREATE PUBLICATION pg_transactional_inbox_pub FOR TABLE public.inbox WITH (publish = 'insert');
select pg_create_logical_replication_slot('pg_transactional_inbox_slot', 'pgoutput');
```

## Implementing the transactional outbox producer

The following code shows the producer side of the transactional outbox pattern.
The two main functions are the `initializeOutboxService` to listen to the WAL
messages when an outbox message was written to the outbox table. And the
`initializeOutboxMessageStore` generator function to store outgoing messages in
the outbox table (for a specific aggregate type and event type).

```TypeScript
import {
  initializeOutboxService,
  OutboxMessage,
  initializeOutboxMessageStore,
} from 'pg-transactional-outbox';
import process from 'node:process';
import { Client } from 'pg';

(async () => {
  // Initialize the actual message publisher e.g. publish the message via RabbitMQ
  const messagePublisher = async (message: OutboxMessage): Promise<void> => {
    // In the simplest case the message can be sent via inter process communication
    process.send?.(message);
  };

  // Initialize and start the outbox subscription. This service receives all the
  // outbox table inserts from the WAL. It executes the messagePublisher function
  // with every received outbox message. It cares for the at least once delivery.
  initializeOutboxService(
    {
      pgReplicationConfig: {
        host: 'localhost',
        port: 5432,
        user: 'db_outbox',
        password: 'db_outbox_password',
        database: 'pg_transactional_outbox',
      },
      settings: {
        outboxSchema: 'public',
        postgresOutboxPub: 'pg_transactional_outbox_pub',
        postgresOutboxSlot: 'pg_transactional_outbox_slot',
      },
    },
    messagePublisher,
  );

  // Initialize the outbox storage function. This function encapsulates the
  // aggregate type (movie) and the event type (movie_created). It will be
  // called to insert the outgoing message into the outbox as part of the DB
  // transaction that is responsible for this event.
  const storeOutboxMessage = initializeOutboxMessageStore('movie', 'movie_created', {
    outboxSchema: 'public',
  });

  // The actual business logic generates in this example a new movie in the DB
  // and wants to reliably send a "movie_created" event.
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
handler queries/mutations, and to finally mark the inbox message as processed in
the database.

```TypeScript
import {
  InboxMessage,
  initializeInboxMessageStorage,
  initializeInboxService,
  OutboxMessage,
} from '../../../../lib/src';
import { fork } from 'child_process';
import { ClientBase } from 'pg';

/** The main entry point of the message producer. */
(async () => {
  // Initialize the inbox message storage to store incoming messages in the inbox
  const storeInboxMessage = await initializeInboxMessageStorage({
    // Use the non-replication DB role to insert inbox rows
    pgConfig: {
      host: 'localhost',
      port: 5432,
      user: 'db_login_inbox',
      password: 'db_login_inbox_password',
      database: 'pg_transactional_inbox',
    },
    settings: {
      inboxSchema: 'public',
    },
  });

  // Initialize the message receiver e.g. based on RabbitMQ
  // In the simplest scenario use the inter process communication:
  process.on('message', async (message: OutboxMessage) => {
    await storeInboxMessage(message);
  });

  // Initialize and start the inbox subscription
  initializeInboxService(
    {
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
        inboxSchema: 'public',
        postgresInboxPub: 'pg_transactional_inbox_pub',
        postgresInboxSlot: 'pg_transactional_outbox_slot',
      },
    },
    // This array holds a list of all message handlers for all the aggregate
    // and event types. More than one handler can be configured for the same
    // aggregate type and event type.
    [
      {
        aggregateType: 'movie',
        eventType: 'movie_created',
        handle: async (
          message: InboxMessage,
          client: ClientBase,
        ): Promise<void> => {
          // Execute the message handler logic using the same database
          // transaction as the inbox message acknowledgement.
          const { payload } = message;
          if (typeof payload === 'object' && payload !== null &&
            'id' in payload && typeof payload.id === 'string' &&
            'title' in payload && typeof payload.title === 'string'
          ) {
            await client.query(
              `INSERT INTO public.published_movies (id, title) VALUES ($1, $2)`,
              [payload.id, payload.title],
            );
          }
        },
      },
    ],
  );
})();
```

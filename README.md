# pg-transactional-outbox

The transactional outbox and transactional inbox patterns are used to ensure
exactly-once processing of messages in event-driven architectures, where the
loss or duplication of a message could have significant consequences. This
library guarantees that a message is sent at least once, but processed exactly
once, and is useful in scenarios where the reliability and atomicity of message
processing is important. This pattern is an alternative to distributed
transactions using a two-phase commit, which can lead to bottlenecks with a
large number of microservices communicating with each other.

![postgresql_outbox_pattern](https://user-images.githubusercontent.com/9946441/211221740-a10d3c0b-dfa9-4c4e-84fb-068f6e63aaac.jpeg)
_Message delivery via the PostgreSQL-based transactional outbox pattern
(generated with Midjourney)_

The `pg-transactional-outbox` is a library that implements the transactional
outbox and transactional inbox pattern for PostgreSQL. It allows you to reliably
send and receive messages within the context of a PostgreSQL transaction,
ensuring that messages are only sent or received if the transaction is
committed.

The `./lib` folder contains the `pg-transactional-outbox` library and unit
tests. You can find an introduction to the transactional outbox and inbox
pattern in the [README.md](./lib/README.md). This should likely be your starting
point.

## Data Flow

![postgresql_outbox_pattern](https://user-images.githubusercontent.com/9946441/215294397-b9b622a1-f923-4d28-a3e0-2ca9f849b63b.png)
_Components involved in the transactional outbox and inbox pattern
implementation_

## Outbox

The `initializeOutboxMessageStorage` function is a generator function to store
outbox messages for a specific aggregate type (e.g. a movie or an order) and a
corresponding message type (e.g. movie_created or order_cancelled). The
generated function can then be used to store a message in the outbox table. Each
outbox item consists of the mentioned aggregate type and message type. And it
includes the aggregate unique identifier (e.g. the movie or order ID), a unique
message identifier (e.g. a UUID), and the message payload. The payload contains
the actual data that should be made available to the message consumers.

This function must be used as part of a PostgreSQL transaction together with the
data mutations that were the reason for sending this message.

The outbox service that gets notified when new outbox rows are created is
initialized via the `initializeOutboxService` function. The configuration input
parameter includes the connection details to the PostgreSQL database
(`pgReplicationConfig`) with the role that has the "replication" attribute. The
configuration also includes the `settings` for the database schema/table for the
outbox table, the name of the used PostgreSQL publication, and the name of the
used PostgreSQL logical replication slot. The second parameter is the callback
function which is executed whenever a new outbox message arrives. The
implementation of this functionality must provide the logic to send the message
via e.g. RabbitMQ or Kafka.

## Inbox

The counterpart to the outbox message store is the
`initializeInboxMessageStorage` function that is used by the actual message
receiver like a RabbitMQ-based message handler to store the incoming message
(which was based on an outbox message) in the inbox table. The database role for
this connection needs insert permissions to the inbox table.

The other central function is `initializeInboxService`. It takes a configuration
object with one database connection (`pgReplicationConfig`) based on a user with
the replication attribute to receive notifications when a new inbox message was
created. And a second database connection (`pgConfig`) to open a database
transaction to process the inbox message and the message handler data changes.
The configuration includes also the `settings` for database schema and table
name of the inbox table, the name of the used PostgreSQL publication, and the
name of the used PostgreSQL logical replication slot.

## General

The library includes error logging and some trace/warning logs. By default, it
uses a `pino` logger instance. You can use the `setLogger` interface to provide
your own `pino` logger instance or another logger that satisfies the pino
`BaseLogger` interface.

The `executeTransaction` functionality could be helpful when implementing the
consumer to store both the outbox message and other data as part of the same
transaction. It takes care to commit and roll back the transaction and release
the client back to the pool.

## Integration Tests

You can find the integration tests in the `./__tests__` folder. This folder
includes outbox service and inbox service integration tests that use a
PostgreSQL instance for the actual tests. Check the
[README.md](./__tests__/README.md) file for further details.

## Examples

The `./examples` folder contains example implementations using actual message
brokers like RabbitMQ.

Currently, there is an example of how to implement the transactional outbox and
inbox pattern when using RabbitMQ as the messaging broker. You can find more
details in the [README.md](./examples/rabbitmq/README.md).

## Windows-only possible isse

It is possible when running `yarn` to encounter an error
`Unable to detect compiler type` on windows machines. In this case
`yarn --ignore-optional` can be used to work around this.

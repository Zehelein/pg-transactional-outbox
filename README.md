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

## Library

This `pg-transactional-outbox` library implements the mentioned transactional
outbox and transactional inbox pattern for PostgreSQL. It stores the outgoing
messages reliably as part of your business logic DB transactions. It stores
incoming messages exactly once and lets your handlers process them exactly once.

The `./lib` folder contains the `pg-transactional-outbox` library and unit
tests. You can find an introduction to the transactional outbox and inbox
pattern in the [README.md](./lib/README.md). This should be your starting point.

## Integration Tests

You can find the integration tests in the `./__tests__` folder. This folder
includes outbox listener and inbox listener integration tests that use a
PostgreSQL instance for the actual tests. Check the
[README.md](./__tests__/README.md) file for further details.

## Examples

The `./examples` folder contains example implementations using actual message
brokers like RabbitMQ.

Currently, there is an example of how to implement the transactional outbox and
inbox pattern when using RabbitMQ as the messaging broker. You can find more
details in the [README.md](./examples/rabbitmq/README.md).

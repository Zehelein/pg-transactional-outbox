# pg-transactional-outbox tests

The `pg-transactional-outbox` includes unit tests that you can run from within
the package.json. As the library heavily depends on the PostgreSQL logical
replication implementation this test project offers actual integration tests.

## Integration Tests

The file `outbox-inbox.spec.ts` starts up its own PostgreSQL database with the
help of the testcontainers library. The integration test file includes tests for
the happy path as well as attempts with retries in case the error handling
throws an error. The PostgreSQL service is meant to be online without any
interruptions for these tests.

The other integration test checks outages of the underlying PostgreSQL database.
Those tests are located in the `outbox-inbox-resilience.spec.ts` file. It uses
again the testcontainers library but stops the PostgreSQL service at some point.
It verifies that the messages are fully delivered once the PostgreSQL service
starts up again.

Both of those files can be executed from the `package.json` by executing the
`test` script.

## Manual logical replication test

The file `logical-rep-service-test.ts` contains a manual setup of the used
`pg-logical-replication`. This file is included to manually check the behavior
of the LogicalReplicationService in case of PostgreSQL outages and how it
recovers (or not) from different kinds of errors.

This test reuses the outbox service database that is created with the RabbitMQ
based example implementation.

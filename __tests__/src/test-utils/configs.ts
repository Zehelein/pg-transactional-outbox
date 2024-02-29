import { ClientConfig } from 'pg';
import {
  PollingListenerConfig,
  ReplicationListenerConfig,
} from 'pg-transactional-outbox';

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export const getConfigs = (port: number) => {
  const handlerConnection: ClientConfig = {
    host: 'localhost',
    port,
    database: 'pg_transactional_outbox_inbox_tests',
    user: 'db_handler_tests',
    password: 'db_handler_tests_password',
  };
  const inboxConfig: ReplicationListenerConfig & PollingListenerConfig = {
    outboxOrInbox: 'inbox',
    dbHandlerConfig: handlerConnection,
    dbListenerConfig: {
      ...handlerConnection,
      user: 'db_inbox_listener_tests',
      password: 'db_inbox_listener_tests_password',
    },
    settings: {
      dbSchema: 'inbox',
      dbTable: 'inbox',
      maxAttempts: 5,
      maxPoisonousAttempts: 3,
      enableMaxAttemptsProtection: true,
      enablePoisonousMessageProtection: true,
      // Replication
      dbPublication: 'pg_transactional_inbox_tests_pub',
      dbReplicationSlot: 'pg_transactional_inbox_tests_slot',
      restartDelayInMs: 1,
      // Polling
      nextMessagesBatchSize: 2,
      nextMessagesFunctionName: 'next_test_inbox_messages',
      nextMessagesFunctionSchema: 'inbox',
      nextMessagesPollingIntervalInMs: 50,
      nextMessagesLockInMs: 100,
    },
  };

  const outboxConfig: ReplicationListenerConfig & PollingListenerConfig = {
    outboxOrInbox: 'outbox',
    dbHandlerConfig: handlerConnection,
    dbListenerConfig: {
      ...handlerConnection,
      user: 'db_outbox_listener_tests',
      password: 'db_outbox_listener_tests_password',
    },
    settings: {
      dbSchema: 'outbox',
      dbTable: 'outbox',
      enablePoisonousMessageProtection: false,
      enableMaxAttemptsProtection: false,
      // Replication
      dbPublication: 'pg_transactional_outbox_tests_pub',
      dbReplicationSlot: 'pg_transactional_outbox_tests_slot',
      // Polling
      nextMessagesBatchSize: 2,
      nextMessagesFunctionName: 'next_test_outbox_messages',
      nextMessagesFunctionSchema: 'outbox',
      nextMessagesPollingIntervalInMs: 50,
      nextMessagesLockInMs: 100,
    },
  };

  return {
    handlerConnection,
    outboxConfig,
    inboxConfig,
  };
};

export type TestConfigs = ReturnType<typeof getConfigs>;

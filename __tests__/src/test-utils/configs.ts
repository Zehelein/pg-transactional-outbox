import { ClientConfig } from 'pg';
import { ReplicationConfig } from 'pg-transactional-outbox';

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export const getConfigs = (port: number) => {
  const handlerConnection: ClientConfig = {
    host: 'localhost',
    port,
    database: 'pg_transactional_outbox_inbox_tests',
    user: 'db_handler_tests',
    password: 'db_handler_tests_password',
  };
  const inboxConfig: ReplicationConfig = {
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
      postgresPub: 'pg_transactional_inbox_tests_pub',
      postgresSlot: 'pg_transactional_inbox_tests_slot',
      restartDelay: 1,
      maxAttempts: 5,
      maxPoisonousAttempts: 3,
    },
  };

  const outboxConfig: ReplicationConfig = {
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
      postgresPub: 'pg_transactional_outbox_tests_pub',
      postgresSlot: 'pg_transactional_outbox_tests_slot',
    },
  };

  return {
    handlerConnection: handlerConnection,
    outboxConfig,
    inboxConfig,
  };
};

export type TestConfigs = ReturnType<typeof getConfigs>;

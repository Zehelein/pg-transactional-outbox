import { ClientConfig } from 'pg';
import { ReplicationConfig } from 'pg-transactional-outbox';

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export const getConfigs = (port: number) => {
  const loginConnection: ClientConfig = {
    host: 'localhost',
    port,
    database: 'pg_transactional_outbox_inbox_tests',
    user: 'db_login_tests',
    password: 'db_login_tests_password',
  };
  const inboxConfig: ReplicationConfig = {
    outboxOrInbox: 'inbox',
    dbHandlerConfig: loginConnection,
    dbListenerConfig: {
      ...loginConnection,
      user: 'db_inbox_tests',
      password: 'db_inbox_tests_password',
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
    dbHandlerConfig: loginConnection,
    dbListenerConfig: {
      ...loginConnection,
      user: 'db_outbox_tests',
      password: 'db_outbox_tests_password',
    },
    settings: {
      dbSchema: 'outbox',
      dbTable: 'outbox',
      postgresPub: 'pg_transactional_outbox_tests_pub',
      postgresSlot: 'pg_transactional_outbox_tests_slot',
    },
  };

  return {
    loginConnection,
    outboxConfig,
    inboxConfig,
  };
};

export type TestConfigs = ReturnType<typeof getConfigs>;

import { ClientConfig } from 'pg';
import {
  InboxConfig,
  InboxServiceConfig,
  OutboxConfig,
  OutboxServiceConfig,
} from 'pg-transactional-outbox';

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export const getConfigs = (port: number) => {
  const loginConnection: ClientConfig = {
    host: 'localhost',
    port,
    database: 'pg_transactional_outbox_inbox_tests',
    user: 'db_login_tests',
    password: 'db_login_tests_password',
  };
  const inboxConfig: InboxConfig = {
    pgConfig: loginConnection,
    settings: {
      inboxSchema: 'inbox',
    },
  };
  const inboxServiceConfig: InboxServiceConfig = {
    pgConfig: loginConnection,
    pgReplicationConfig: {
      ...loginConnection,
      user: 'db_inbox_tests',
      password: 'db_inbox_tests_password',
    },
    settings: {
      inboxSchema: inboxConfig.settings.inboxSchema,
      postgresInboxPub: 'pg_transactional_inbox_tests_pub',
      postgresInboxSlot: 'pg_transactional_inbox_tests_slot',
    },
  };

  const outboxConfig: OutboxConfig = {
    outboxSchema: 'outbox',
  };

  const outboxServiceConfig: OutboxServiceConfig = {
    pgReplicationConfig: {
      ...loginConnection,
      user: 'db_outbox_tests',
      password: 'db_outbox_tests_password',
    },
    settings: {
      outboxSchema: outboxConfig.outboxSchema,
      postgresOutboxPub: 'pg_transactional_outbox_tests_pub',
      postgresOutboxSlot: 'pg_transactional_outbox_tests_slot',
    },
  };

  return {
    loginConnection,
    outboxServiceConfig,
    outboxConfig,
    inboxServiceConfig,
    inboxConfig,
  };
};

export type TestConfigs = ReturnType<typeof getConfigs>;

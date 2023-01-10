import { ClientConfig } from 'pg';
import {
  InboxConfig,
  InboxServiceConfig,
  OutboxConfig,
  OutboxServiceConfig,
} from 'pg-transactional-outbox';

export const defaultLoginConnection: ClientConfig = {
  host: 'localhost',
  port: 15432,
  database: 'pg_transactional_outbox_inbox_tests',
  user: 'db_login_tests',
  password: 'db_login_tests_password',
};

export const defaultInboxConfig: InboxConfig = {
  pgConfig: defaultLoginConnection,
  settings: {
    inboxSchema: 'inbox',
  },
};

export const defaultInboxServiceConfig: InboxServiceConfig = {
  pgConfig: defaultInboxConfig.pgConfig,
  pgReplicationConfig: {
    ...defaultLoginConnection,
    user: 'db_inbox_tests',
    password: 'db_inbox_tests_password',
  },
  settings: {
    inboxSchema: defaultInboxConfig.settings.inboxSchema,
    postgresInboxPub: 'pg_transactional_inbox_tests_pub',
    postgresInboxSlot: 'pg_transactional_inbox_tests_slot',
  },
};

export const defaultOutboxConfig: OutboxConfig = {
  outboxSchema: 'outbox',
};

export const defaultOutboxServiceConfig: OutboxServiceConfig = {
  pgReplicationConfig: {
    ...defaultLoginConnection,
    user: 'db_outbox_tests',
    password: 'db_outbox_tests_password',
  },
  settings: {
    outboxSchema: defaultOutboxConfig.outboxSchema,
    postgresOutboxPub: 'pg_transactional_outbox_tests_pub',
    postgresOutboxSlot: 'pg_transactional_outbox_tests_slot',
  },
};

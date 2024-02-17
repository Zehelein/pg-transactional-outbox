import {
  FullReplicationListenerConfig,
  ReplicationListenerConfig,
  applyDefaultReplicationListenerConfigValues,
  getInboxReplicationListenerSettings,
  getOutboxReplicationListenerSettings,
  printInboxReplicationListenerEnvVariables,
  printOutboxReplicationListenerEnvVariables,
} from './config';

describe('Replication listener settings', () => {
  describe('applyDefaultReplicationListenerConfigValues', () => {
    const baseConfig: ReplicationListenerConfig = {
      outboxOrInbox: 'outbox',
      dbListenerConfig: { connectionString: 'my-listener-connection' },
      settings: {
        dbSchema: 'public',
        dbTable: 'my-table',
        enableMaxAttemptsProtection: true,
        enablePoisonousMessageProtection: false,
        dbPublication: 'pub',
        dbReplicationSlot: 'slot',
      },
    };

    it('should return a configuration with default values applied for missing values', () => {
      const result = applyDefaultReplicationListenerConfigValues(baseConfig);
      const expected: FullReplicationListenerConfig = {
        ...baseConfig,
        dbHandlerConfig: baseConfig.dbListenerConfig,
        settings: {
          dbTable: 'my-table',
          dbSchema: 'public',
          messageProcessingTimeoutInMs: 15_000,
          maxAttempts: 5,
          enableMaxAttemptsProtection: true,
          maxPoisonousAttempts: 3,
          enablePoisonousMessageProtection: false,
          messageCleanupIntervalInMs: 300000,
          messageCleanupProcessedInSec: 604800,
          messageCleanupAbandonedInSec: 1209600,
          messageCleanupAllInSec: 5184000,
          dbPublication: 'pub',
          dbReplicationSlot: 'slot',
          restartDelayInMs: 250,
          restartDelaySlotInUseInMs: 10000,
        },
      };
      expect(result).toEqual(expected);
    });

    it('should keep full input config without applying defaults', () => {
      const fullConfig: FullReplicationListenerConfig = {
        outboxOrInbox: 'outbox',
        dbHandlerConfig: { connectionString: 'my-handler-connection' },
        dbListenerConfig: { connectionString: 'my-listener-connection' },
        settings: {
          dbTable: 'my-table',
          dbSchema: 'private',
          messageProcessingTimeoutInMs: 10,
          maxAttempts: 20,
          enableMaxAttemptsProtection: false,
          maxPoisonousAttempts: 30,
          enablePoisonousMessageProtection: false,
          messageCleanupIntervalInMs: 40,
          messageCleanupProcessedInSec: 50,
          messageCleanupAbandonedInSec: 60,
          messageCleanupAllInSec: 70,
          dbPublication: 'full_pub',
          dbReplicationSlot: 'full_slot',
          restartDelayInMs: 80,
          restartDelaySlotInUseInMs: 90,
        },
      };
      const result = applyDefaultReplicationListenerConfigValues(fullConfig);
      expect(result).toStrictEqual(fullConfig);
    });
  });

  describe('getInboxReplicationListenerSettings', () => {
    // Mocking environment object
    const mockEnv = {
      TRX_INBOX_DB_SCHEMA: 'test_schema',
      TRX_INBOX_DB_TABLE: 'test_table',
      TRX_INBOX_MESSAGE_PROCESSING_TIMEOUT_IN_MS: '123456',
      TRX_INBOX_MAX_ATTEMPTS: '123',
      TRX_INBOX_ENABLE_MAX_ATTEMPTS_PROTECTION: 'false',
      TRX_INBOX_MAX_POISONOUS_ATTEMPTS: '456',
      TRX_INBOX_ENABLE_POISONOUS_MESSAGE_PROTECTION: 'false',
      TRX_INBOX_MESSAGE_CLEANUP_INTERVAL_IN_MS: '789',
      TRX_INBOX_MESSAGE_CLEANUP_PROCESSED_IN_SEC: '321',
      TRX_INBOX_MESSAGE_CLEANUP_ABANDONED_IN_SEC: '654',
      TRX_INBOX_MESSAGE_CLEANUP_ALL_IN_SEC: '987',
      // specific replication settings
      TRX_INBOX_RESTART_DELAY_IN_MS: '1234',
      TRX_INBOX_RESTART_DELAY_SLOT_IN_USE_IN_MS: '56789',
      TRX_INBOX_DB_PUBLICATION: 'test_transactional_inbox_pub',
      TRX_INBOX_DB_REPLICATION_SLOT: 'test_transactional_inbox_slot',
    };

    it('should return listener settings with all default values', () => {
      const expectedSettings = {
        dbSchema: 'public',
        dbTable: 'inbox',
        enableMaxAttemptsProtection: true,
        enablePoisonousMessageProtection: true,
        maxAttempts: 5,
        maxPoisonousAttempts: 3,
        messageCleanupAbandonedInSec: 1209600,
        messageCleanupAllInSec: 5184000,
        messageCleanupIntervalInMs: 300000,
        messageCleanupProcessedInSec: 604800,
        messageProcessingTimeoutInMs: 15000,

        dbPublication: 'pg_transactional_inbox_pub',
        dbReplicationSlot: 'pg_transactional_inbox_slot',
        restartDelayInMs: 250,
        restartDelaySlotInUseInMs: 10000,
      };

      const settings = getInboxReplicationListenerSettings({});

      expect(settings).toEqual(expectedSettings);
    });

    it('should return listener settings with provided values', () => {
      const expectedSettings = {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        enableMaxAttemptsProtection: false,
        enablePoisonousMessageProtection: false,
        maxAttempts: 123,
        maxPoisonousAttempts: 456,
        messageCleanupAbandonedInSec: 654,
        messageCleanupAllInSec: 987,
        messageCleanupIntervalInMs: 789,
        messageCleanupProcessedInSec: 321,
        messageProcessingTimeoutInMs: 123456,

        dbPublication: 'test_transactional_inbox_pub',
        dbReplicationSlot: 'test_transactional_inbox_slot',
        restartDelayInMs: 1234,
        restartDelaySlotInUseInMs: 56789,
      };

      const settings = getInboxReplicationListenerSettings(mockEnv);

      expect(settings).toEqual(expectedSettings);
    });

    it('should return listener settings with normal and fallback values', () => {
      const mixedEnv = {
        TRX_INBOX_DB_SCHEMA: 'inbox_schema',
        TRX_INBOX_DB_TABLE: 'inbox_table',
        TRX_MESSAGE_PROCESSING_TIMEOUT_IN_MS: '30000',
        TRX_INBOX_MAX_ATTEMPTS: '7',
        TRX_ENABLE_MAX_ATTEMPTS_PROTECTION: 'true',
        TRX_INBOX_MAX_POISONOUS_ATTEMPTS: '4',
        TRX_INBOX_ENABLE_POISONOUS_MESSAGE_PROTECTION: 'true',
        TRX_MESSAGE_CLEANUP_INTERVAL_IN_MS: '20000',
        TRX_MESSAGE_CLEANUP_PROCESSED_IN_SEC: '300000',
        TRX_MESSAGE_CLEANUP_ABANDONED_IN_SEC: '400000',
        TRX_MESSAGE_CLEANUP_ALL_IN_SEC: '500000',

        TRX_RESTART_DELAY_IN_MS: '9876',
        TRX_RESTART_DELAY_SLOT_IN_USE_IN_MS: '12345',
        TRX_INBOX_DB_PUBLICATION: 'test_transactional_inbox_pub',
        TRX_INBOX_DB_REPLICATION_SLOT: 'test_transactional_inbox_slot',
      };

      const expectedSettings = {
        dbSchema: 'inbox_schema',
        dbTable: 'inbox_table',
        messageProcessingTimeoutInMs: 30000,
        maxAttempts: 7,
        enableMaxAttemptsProtection: true,
        maxPoisonousAttempts: 4,
        enablePoisonousMessageProtection: true,
        messageCleanupIntervalInMs: 20000,
        messageCleanupProcessedInSec: 300000,
        messageCleanupAbandonedInSec: 400000,
        messageCleanupAllInSec: 500000,

        dbPublication: 'test_transactional_inbox_pub',
        dbReplicationSlot: 'test_transactional_inbox_slot',
        restartDelayInMs: 9876,
        restartDelaySlotInUseInMs: 12345,
      };

      const settings = getInboxReplicationListenerSettings(mixedEnv);

      expect(settings).toEqual(expectedSettings);
    });
  });

  describe('getOutboxReplicationListenerSettings', () => {
    // Mocking environment object
    const mockEnv = {
      TRX_OUTBOX_DB_SCHEMA: 'test_schema',
      TRX_OUTBOX_DB_TABLE: 'test_table',
      TRX_OUTBOX_MESSAGE_PROCESSING_TIMEOUT_IN_MS: '123456',
      TRX_OUTBOX_MAX_ATTEMPTS: '123',
      TRX_OUTBOX_ENABLE_MAX_ATTEMPTS_PROTECTION: 'false',
      TRX_OUTBOX_MAX_POISONOUS_ATTEMPTS: '456',
      TRX_OUTBOX_ENABLE_POISONOUS_MESSAGE_PROTECTION: 'false',
      TRX_OUTBOX_MESSAGE_CLEANUP_INTERVAL_IN_MS: '789',
      TRX_OUTBOX_MESSAGE_CLEANUP_PROCESSED_IN_SEC: '321',
      TRX_OUTBOX_MESSAGE_CLEANUP_ABANDONED_IN_SEC: '654',
      TRX_OUTBOX_MESSAGE_CLEANUP_ALL_IN_SEC: '987',
      // specific replication settings
      TRX_OUTBOX_RESTART_DELAY_IN_MS: '1234',
      TRX_OUTBOX_RESTART_DELAY_SLOT_IN_USE_IN_MS: '56789',
      TRX_OUTBOX_DB_PUBLICATION: 'test_transactional_outbox_pub',
      TRX_OUTBOX_DB_REPLICATION_SLOT: 'test_transactional_outbox_slot',
    };

    it('should return listener settings with all default values', () => {
      const expectedSettings = {
        dbSchema: 'public',
        dbTable: 'outbox',
        enableMaxAttemptsProtection: false,
        enablePoisonousMessageProtection: false,
        maxAttempts: 5,
        maxPoisonousAttempts: 3,
        messageCleanupAbandonedInSec: 1209600,
        messageCleanupAllInSec: 5184000,
        messageCleanupIntervalInMs: 300000,
        messageCleanupProcessedInSec: 604800,
        messageProcessingTimeoutInMs: 15000,

        dbPublication: 'pg_transactional_outbox_pub',
        dbReplicationSlot: 'pg_transactional_outbox_slot',
        restartDelayInMs: 250,
        restartDelaySlotInUseInMs: 10000,
      };

      const settings = getOutboxReplicationListenerSettings({});

      expect(settings).toEqual(expectedSettings);
    });

    it('should return listener settings with provided values', () => {
      const expectedSettings = {
        dbSchema: 'test_schema',
        dbTable: 'test_table',
        enableMaxAttemptsProtection: false,
        enablePoisonousMessageProtection: false,
        maxAttempts: 123,
        maxPoisonousAttempts: 456,
        messageCleanupAbandonedInSec: 654,
        messageCleanupAllInSec: 987,
        messageCleanupIntervalInMs: 789,
        messageCleanupProcessedInSec: 321,
        messageProcessingTimeoutInMs: 123456,

        dbPublication: 'test_transactional_outbox_pub',
        dbReplicationSlot: 'test_transactional_outbox_slot',
        restartDelayInMs: 1234,
        restartDelaySlotInUseInMs: 56789,
      };

      const settings = getOutboxReplicationListenerSettings(mockEnv);

      expect(settings).toEqual(expectedSettings);
    });
  });

  describe('print replication listener settings', () => {
    it('printInboxReplicationListenerEnvVariables', () => {
      const settings = printInboxReplicationListenerEnvVariables();
      const expected = /* js */ `# Inbox listener variables
TRX_INBOX_DB_SCHEMA=public
TRX_DB_SCHEMA=public
TRX_INBOX_MESSAGE_PROCESSING_TIMEOUT_IN_MS=15000
TRX_MESSAGE_PROCESSING_TIMEOUT_IN_MS=15000
TRX_INBOX_MAX_ATTEMPTS=5
TRX_MAX_ATTEMPTS=5
TRX_INBOX_MAX_POISONOUS_ATTEMPTS=3
TRX_MAX_POISONOUS_ATTEMPTS=3
TRX_INBOX_MESSAGE_CLEANUP_INTERVAL_IN_MS=300000
TRX_MESSAGE_CLEANUP_INTERVAL_IN_MS=300000
TRX_INBOX_MESSAGE_CLEANUP_PROCESSED_IN_SEC=604800
TRX_MESSAGE_CLEANUP_PROCESSED_IN_SEC=604800
TRX_INBOX_MESSAGE_CLEANUP_ABANDONED_IN_SEC=1209600
TRX_MESSAGE_CLEANUP_ABANDONED_IN_SEC=1209600
TRX_INBOX_MESSAGE_CLEANUP_ALL_IN_SEC=5184000
TRX_MESSAGE_CLEANUP_ALL_IN_SEC=5184000
TRX_INBOX_DB_TABLE=inbox
TRX_INBOX_ENABLE_MAX_ATTEMPTS_PROTECTION=true
TRX_INBOX_ENABLE_POISONOUS_MESSAGE_PROTECTION=true

# Inbox replication listener variables
TRX_INBOX_RESTART_DELAY_IN_MS=250
TRX_RESTART_DELAY_IN_MS=250
TRX_INBOX_RESTART_DELAY_SLOT_IN_USE_IN_MS=10000
TRX_RESTART_DELAY_SLOT_IN_USE_IN_MS=10000
TRX_INBOX_DB_PUBLICATION=pg_transactional_inbox_pub
TRX_INBOX_DB_REPLICATION_SLOT=pg_transactional_inbox_slot
`;
      expect(settings).toBe(expected);
    });

    it('printOutboxReplicationListenerEnvVariables', () => {
      const settings = printOutboxReplicationListenerEnvVariables();
      const expected = /* js */ `# Outbox listener variables
TRX_OUTBOX_DB_SCHEMA=public
TRX_DB_SCHEMA=public
TRX_OUTBOX_MESSAGE_PROCESSING_TIMEOUT_IN_MS=15000
TRX_MESSAGE_PROCESSING_TIMEOUT_IN_MS=15000
TRX_OUTBOX_MAX_ATTEMPTS=5
TRX_MAX_ATTEMPTS=5
TRX_OUTBOX_MAX_POISONOUS_ATTEMPTS=3
TRX_MAX_POISONOUS_ATTEMPTS=3
TRX_OUTBOX_MESSAGE_CLEANUP_INTERVAL_IN_MS=300000
TRX_MESSAGE_CLEANUP_INTERVAL_IN_MS=300000
TRX_OUTBOX_MESSAGE_CLEANUP_PROCESSED_IN_SEC=604800
TRX_MESSAGE_CLEANUP_PROCESSED_IN_SEC=604800
TRX_OUTBOX_MESSAGE_CLEANUP_ABANDONED_IN_SEC=1209600
TRX_MESSAGE_CLEANUP_ABANDONED_IN_SEC=1209600
TRX_OUTBOX_MESSAGE_CLEANUP_ALL_IN_SEC=5184000
TRX_MESSAGE_CLEANUP_ALL_IN_SEC=5184000
TRX_OUTBOX_DB_TABLE=outbox
TRX_OUTBOX_ENABLE_MAX_ATTEMPTS_PROTECTION=false
TRX_OUTBOX_ENABLE_POISONOUS_MESSAGE_PROTECTION=false

# Outbox replication listener variables
TRX_OUTBOX_RESTART_DELAY_IN_MS=250
TRX_RESTART_DELAY_IN_MS=250
TRX_OUTBOX_RESTART_DELAY_SLOT_IN_USE_IN_MS=10000
TRX_RESTART_DELAY_SLOT_IN_USE_IN_MS=10000
TRX_OUTBOX_DB_PUBLICATION=pg_transactional_outbox_pub
TRX_OUTBOX_DB_REPLICATION_SLOT=pg_transactional_outbox_slot
`;

      expect(settings).toBe(expected);
    });
  });
});

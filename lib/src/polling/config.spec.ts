import {
  getInboxPollingListenerSettings,
  getOutboxPollingListenerSettings,
  printInboxPollingListenerEnvVariables,
  printOutboxPollingListenerEnvVariables,
} from './config';

describe('Polling listener settings', () => {
  describe('getInboxPollingListenerSettings', () => {
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
      // specific polling settings
      TRX_INBOX_NEXT_MESSAGES_FUNCTION_SCHEMA: 'test_inbox_schema',
      TRX_INBOX_NEXT_MESSAGES_FUNCTION_NAME: 'next_test_inbox_messages',
      TRX_INBOX_NEXT_MESSAGES_BATCH_SIZE: '27',
      TRX_INBOX_NEXT_MESSAGES_LOCK_IN_MS: '42',
      TRX_INBOX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS: '123',
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

        nextMessagesBatchSize: 5,
        nextMessagesFunctionName: 'next_inbox_messages',
        nextMessagesFunctionSchema: 'public',
        nextMessagesLockInMs: 5000,
        nextMessagesPollingIntervalInMs: 500,
      };

      const settings = getInboxPollingListenerSettings({});

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

        nextMessagesBatchSize: 27,
        nextMessagesFunctionName: 'next_test_inbox_messages',
        nextMessagesFunctionSchema: 'test_inbox_schema',
        nextMessagesLockInMs: 42,
        nextMessagesPollingIntervalInMs: 123,
      };

      const settings = getInboxPollingListenerSettings(mockEnv);

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
        // specific polling settings
        TRX_INBOX_NEXT_MESSAGES_FUNCTION_SCHEMA: 'test_inbox_schema',
        TRX_INBOX_NEXT_MESSAGES_FUNCTION_NAME: 'next_test_inbox_messages',
        TRX_NEXT_MESSAGES_BATCH_SIZE: '27',
        TRX_NEXT_MESSAGES_LOCK_IN_MS: '42',
        TRX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS: '123',
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

        nextMessagesBatchSize: 27,
        nextMessagesFunctionName: 'next_test_inbox_messages',
        nextMessagesFunctionSchema: 'test_inbox_schema',
        nextMessagesLockInMs: 42,
        nextMessagesPollingIntervalInMs: 123,
      };

      const settings = getInboxPollingListenerSettings(mixedEnv);

      expect(settings).toEqual(expectedSettings);
    });
  });

  describe('getOutboxPollingListenerSettings', () => {
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
      // specific polling settings
      TRX_OUTBOX_NEXT_MESSAGES_FUNCTION_SCHEMA: 'test_outbox_schema',
      TRX_OUTBOX_NEXT_MESSAGES_FUNCTION_NAME: 'next_test_outbox_messages',
      TRX_OUTBOX_NEXT_MESSAGES_BATCH_SIZE: '27',
      TRX_OUTBOX_NEXT_MESSAGES_LOCK_IN_MS: '42',
      TRX_OUTBOX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS: '123',
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

        nextMessagesBatchSize: 5,
        nextMessagesFunctionName: 'next_outbox_messages',
        nextMessagesFunctionSchema: 'public',
        nextMessagesLockInMs: 5000,
        nextMessagesPollingIntervalInMs: 500,
      };

      const settings = getOutboxPollingListenerSettings({});

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

        nextMessagesBatchSize: 27,
        nextMessagesFunctionName: 'next_test_outbox_messages',
        nextMessagesFunctionSchema: 'test_outbox_schema',
        nextMessagesLockInMs: 42,
        nextMessagesPollingIntervalInMs: 123,
      };

      const settings = getOutboxPollingListenerSettings(mockEnv);

      expect(settings).toEqual(expectedSettings);
    });
  });

  describe('print polling listener settings', () => {
    it('printInboxPollingListenerEnvVariables', () => {
      const settings = printInboxPollingListenerEnvVariables();
      const expected = /* sql */ `# Inbox listener variables
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

# Inbox polling listener variables
TRX_INBOX_NEXT_MESSAGES_FUNCTION_SCHEMA=public
TRX_NEXT_MESSAGES_FUNCTION_SCHEMA=public
TRX_INBOX_NEXT_MESSAGES_BATCH_SIZE=5
TRX_NEXT_MESSAGES_BATCH_SIZE=5
TRX_INBOX_NEXT_MESSAGES_LOCK_IN_MS=5000
TRX_NEXT_MESSAGES_LOCK_IN_MS=5000
TRX_INBOX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS=500
TRX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS=500
TRX_INBOX_NEXT_MESSAGES_FUNCTION_NAME=next_inbox_messages
`;
      expect(settings).toBe(expected);
    });

    it('printOutboxPollingListenerEnvVariables', () => {
      const settings = printOutboxPollingListenerEnvVariables();
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

# Outbox polling listener variables
TRX_OUTBOX_NEXT_MESSAGES_FUNCTION_SCHEMA=public
TRX_NEXT_MESSAGES_FUNCTION_SCHEMA=public
TRX_OUTBOX_NEXT_MESSAGES_BATCH_SIZE=5
TRX_NEXT_MESSAGES_BATCH_SIZE=5
TRX_OUTBOX_NEXT_MESSAGES_LOCK_IN_MS=5000
TRX_NEXT_MESSAGES_LOCK_IN_MS=5000
TRX_OUTBOX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS=500
TRX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS=500
TRX_OUTBOX_NEXT_MESSAGES_FUNCTION_NAME=next_outbox_messages
`;

      expect(settings).toBe(expected);
    });
  });
});

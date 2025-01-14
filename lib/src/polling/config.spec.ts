import {
  FullPollingListenerConfig,
  PollingListenerConfig,
  applyDefaultPollingListenerConfigValues,
  getInboxPollingListenerEnvTemplate,
  getInboxPollingListenerSettings,
  getOutboxPollingListenerEnvTemplate,
  getOutboxPollingListenerSettings,
} from './config';

describe('Polling listener settings', () => {
  describe('applyDefaultPollingListenerConfigValues', () => {
    const baseConfig: PollingListenerConfig = {
      outboxOrInbox: 'outbox',
      dbListenerConfig: { connectionString: 'my-listener-connection' },
      settings: {
        dbSchema: 'public',
        dbTable: 'my-table',
        enableMaxAttemptsProtection: true,
        enablePoisonousMessageProtection: false,
        nextMessagesFunctionName: 'next_default_messages_function',
      },
    };

    it('should return a configuration with default values applied for missing values', () => {
      const result = applyDefaultPollingListenerConfigValues(baseConfig);
      const expected: FullPollingListenerConfig = {
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
          nextMessagesFunctionName: 'next_default_messages_function',
          nextMessagesFunctionSchema: 'public',
          nextMessagesBatchSize: 5,
          nextMessagesLockInMs: 5000,
          nextMessagesPollingIntervalInMs: 500,
          maxMessageNotFoundAttempts: 0,
          maxMessageNotFoundDelayInMs: 10,
        },
      };
      expect(result).toEqual(expected);
    });

    it('should keep full input config without applying defaults', () => {
      const fullConfig: FullPollingListenerConfig = {
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
          nextMessagesFunctionName: 'next',
          nextMessagesFunctionSchema: 'private',
          nextMessagesBatchSize: 80,
          nextMessagesLockInMs: 90,
          nextMessagesPollingIntervalInMs: 100,
          maxMessageNotFoundAttempts: 0,
          maxMessageNotFoundDelayInMs: 10,
        },
      };
      const result = applyDefaultPollingListenerConfigValues(fullConfig);
      expect(result).toStrictEqual(fullConfig);
    });
  });

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

  describe('get polling listener settings ENV template', () => {
    it('getInboxPollingListenerEnvTemplate', () => {
      const settings = getInboxPollingListenerEnvTemplate();
      const expected = /* js */ `# | TRX_DB_SCHEMA | string | "public" | The database schema name where the table is located. |
TRX_DB_SCHEMA=public
# | TRX_MESSAGE_PROCESSING_TIMEOUT_IN_MS | number | 15000 | Stop the message handler after this time has passed. |
TRX_MESSAGE_PROCESSING_TIMEOUT_IN_MS=15000
# | TRX_MAX_ATTEMPTS | number | 5 | The maximum number of attempts to handle a message. With max 5 attempts a message is handled once initially and up to four more times for retries. |
TRX_MAX_ATTEMPTS=5
# | TRX_MAX_POISONOUS_ATTEMPTS | number | 3 | The maximum number of times a message should be attempted which was started but did not finish (neither error nor success). |
TRX_MAX_POISONOUS_ATTEMPTS=3
# | TRX_MESSAGE_CLEANUP_INTERVAL_IN_MS | number | 300000 | Time in milliseconds between the execution of the old message cleanups. Set it to zero to disable automatic message cleanup. |
TRX_MESSAGE_CLEANUP_INTERVAL_IN_MS=300000
# | TRX_MESSAGE_CLEANUP_PROCESSED_IN_SEC | number | 604800 | Delete messages that were successfully processed after X seconds. |
TRX_MESSAGE_CLEANUP_PROCESSED_IN_SEC=604800
# | TRX_MESSAGE_CLEANUP_ABANDONED_IN_SEC | number | 1209600 | Delete messages that could not be processed after X seconds. |
TRX_MESSAGE_CLEANUP_ABANDONED_IN_SEC=1209600
# | TRX_MESSAGE_CLEANUP_ALL_IN_SEC | number | 5184000 | Delete all old messages after X seconds. |
TRX_MESSAGE_CLEANUP_ALL_IN_SEC=5184000
# | TRX_INBOX_DB_TABLE | string | "inbox" | The name of the database inbox table. |
TRX_INBOX_DB_TABLE=inbox
# | TRX_INBOX_ENABLE_MAX_ATTEMPTS_PROTECTION | boolean | true | Enable the max attempts protection. |
TRX_INBOX_ENABLE_MAX_ATTEMPTS_PROTECTION=true
# | TRX_INBOX_ENABLE_POISONOUS_MESSAGE_PROTECTION | boolean | true | Enable the max poisonous attempts protection. |
TRX_INBOX_ENABLE_POISONOUS_MESSAGE_PROTECTION=true
# | TRX_NEXT_MESSAGES_FUNCTION_SCHEMA | string | "public" | The database schema of the next messages function. |
TRX_NEXT_MESSAGES_FUNCTION_SCHEMA=public
# | TRX_NEXT_MESSAGES_BATCH_SIZE | number | 5 | The (maximum) amount of messages to retrieve in one query. |
TRX_NEXT_MESSAGES_BATCH_SIZE=5
# | TRX_NEXT_MESSAGES_LOCK_IN_MS | number | 5000 | How long the retrieved messages should be locked before they can be retrieved again. |
TRX_NEXT_MESSAGES_LOCK_IN_MS=5000
# | TRX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS | number | 500 | How often should the next messages function be executed. |
TRX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS=500
# | TRX_INBOX_NEXT_MESSAGES_FUNCTION_NAME | string | "next_inbox_messages" | The database function name to get the next batch of inbox messages. |
TRX_INBOX_NEXT_MESSAGES_FUNCTION_NAME=next_inbox_messages
`;
      expect(settings).toBe(expected);
    });

    it('getOutboxPollingListenerEnvTemplate', () => {
      const settings = getOutboxPollingListenerEnvTemplate();
      const expected = /* js */ `# | TRX_DB_SCHEMA | string | "public" | The database schema name where the table is located. |
TRX_DB_SCHEMA=public
# | TRX_MESSAGE_PROCESSING_TIMEOUT_IN_MS | number | 15000 | Stop the message handler after this time has passed. |
TRX_MESSAGE_PROCESSING_TIMEOUT_IN_MS=15000
# | TRX_MAX_ATTEMPTS | number | 5 | The maximum number of attempts to handle a message. With max 5 attempts a message is handled once initially and up to four more times for retries. |
TRX_MAX_ATTEMPTS=5
# | TRX_MAX_POISONOUS_ATTEMPTS | number | 3 | The maximum number of times a message should be attempted which was started but did not finish (neither error nor success). |
TRX_MAX_POISONOUS_ATTEMPTS=3
# | TRX_MESSAGE_CLEANUP_INTERVAL_IN_MS | number | 300000 | Time in milliseconds between the execution of the old message cleanups. Set it to zero to disable automatic message cleanup. |
TRX_MESSAGE_CLEANUP_INTERVAL_IN_MS=300000
# | TRX_MESSAGE_CLEANUP_PROCESSED_IN_SEC | number | 604800 | Delete messages that were successfully processed after X seconds. |
TRX_MESSAGE_CLEANUP_PROCESSED_IN_SEC=604800
# | TRX_MESSAGE_CLEANUP_ABANDONED_IN_SEC | number | 1209600 | Delete messages that could not be processed after X seconds. |
TRX_MESSAGE_CLEANUP_ABANDONED_IN_SEC=1209600
# | TRX_MESSAGE_CLEANUP_ALL_IN_SEC | number | 5184000 | Delete all old messages after X seconds. |
TRX_MESSAGE_CLEANUP_ALL_IN_SEC=5184000
# | TRX_OUTBOX_DB_TABLE | string | "outbox" | The name of the database outbox table. |
TRX_OUTBOX_DB_TABLE=outbox
# | TRX_OUTBOX_ENABLE_MAX_ATTEMPTS_PROTECTION | boolean | false | Enable the max attempts protection. |
TRX_OUTBOX_ENABLE_MAX_ATTEMPTS_PROTECTION=false
# | TRX_OUTBOX_ENABLE_POISONOUS_MESSAGE_PROTECTION | boolean | false | Enable the max poisonous attempts protection. |
TRX_OUTBOX_ENABLE_POISONOUS_MESSAGE_PROTECTION=false
# | TRX_NEXT_MESSAGES_FUNCTION_SCHEMA | string | "public" | The database schema of the next messages function. |
TRX_NEXT_MESSAGES_FUNCTION_SCHEMA=public
# | TRX_NEXT_MESSAGES_BATCH_SIZE | number | 5 | The (maximum) amount of messages to retrieve in one query. |
TRX_NEXT_MESSAGES_BATCH_SIZE=5
# | TRX_NEXT_MESSAGES_LOCK_IN_MS | number | 5000 | How long the retrieved messages should be locked before they can be retrieved again. |
TRX_NEXT_MESSAGES_LOCK_IN_MS=5000
# | TRX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS | number | 500 | How often should the next messages function be executed. |
TRX_NEXT_MESSAGES_POLLING_INTERVAL_IN_MS=500
# | TRX_OUTBOX_NEXT_MESSAGES_FUNCTION_NAME | string | "next_outbox_messages" | The database function name to get the next batch of outbox messages. |
TRX_OUTBOX_NEXT_MESSAGES_FUNCTION_NAME=next_outbox_messages
`;

      expect(settings).toBe(expected);
    });
  });
});

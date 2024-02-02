import { ClientConfig } from 'pg';
import { OutboxOrInbox } from './definitions';

export interface ListenerConfig {
  /**
   * Defines if this listener is used for the transactional outbox or inbox
   * handling
   */
  outboxOrInbox: OutboxOrInbox;
  /**
   * Database connection details for the message handler logic. The user needs
   * update permission to the outbox or inbox table. Uses the dbListenerConfig
   * if it is not provided.
   */
  dbHandlerConfig?: ClientConfig;
  /**
   * The "pg" library based settings to initialize the PostgreSQL connection for
   * the logical replication listener (with replication permissions) or the
   * polling listener.
   */
  dbListenerConfig: ClientConfig;
  /** Listener specific configurations */
  settings: ListenerSettings;
}

export interface ListenerSettings {
  /** The database schema name where the table is located */
  dbSchema: string;
  /** The database table of the outbox/inbox */
  dbTable: string;
  /**
   * Outbox message sender or the inbox message handlers that do not finish can
   * block further messages from being processed/sent. The timeout
   * (in milliseconds) ensures to continue with the next items. Default is 15s.
   */
  messageProcessingTimeout?: number;
  /**
   * The maximum number of attempts to handle an incoming message.
   * Defaults to 5 which means a message is handled once initially and up to
   * four more times for retries.
   */
  maxAttempts?: number;
  /**
   * Enable max attempts protection. Might be disabled when using it for the
   * outbox scenario. Defaults to true.
   */
  enableMaxAttemptsProtection?: boolean;
  /**
   * Defines the maximum number of times a message is going to be processed
   * when it is (likely) a poisonous message which is causing a server crash.
   * This is used in the default poisonous message retry strategy.
   * It defaults to three.
   */
  maxPoisonousAttempts?: number;
  /**
   * Poisonous message protection is enabled by default or if you set this to
   * true. Enabling it will take a little bit more time but will prevent an
   * infinite service crash loop if there is a poisonous message.
   */
  enablePoisonousMessageProtection?: boolean;
}

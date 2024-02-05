import { ListenerConfig, ListenerSettings } from '../common/base-config';

export interface PollingConfig extends ListenerConfig {
  /** Polling listener specific configurations */
  settings: PollingListenerConfig;
}

export interface PollingListenerConfig extends ListenerSettings {
  /**
   * The name of the schema of the Postgres function to get the next batch of
   * messages. It defaults to the `dbSchema` if it is not provided.
   */
  nextMessagesFunctionSchema: string;
  /**
   * The name of the Postgres function to get the next batch of outbox or inbox
   * messages.
   */
  nextMessagesFunctionName: string;
  /** The batch size for messages to load simultaneously. Default is 5. */
  nestMessagesBatchSize: number;
  /** How long should a message be locked for exclusive processing and error handling (in milliseconds). Default is 5 seconds. */
  nextMessagesLockMs?: number;
}

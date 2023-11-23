import { ClientConfig } from 'pg';

export interface ServiceConfig {
  /**
   * The "pg" library based settings to initialize the PostgreSQL connection for
   * the logical replication service (with replication permissions)
   */
  pgReplicationConfig: ClientConfig;
  /** service specific configurations */
  settings: ServiceConfigSettings;
}

export interface ServiceConfigSettings {
  /** The database schema name where the table is located */
  dbSchema: string;
  /** The database table of the inbox/outbox */
  dbTable: string;
  /** The name of the used PostgreSQL publication */
  postgresPub: string;
  /** The name of the used PostgreSQL logical replication slot */
  postgresSlot: string;
  /** When there is a message processing error it restarts the logical replication subscription with a delay. This setting defines this delay in milliseconds. Default is 250ms. */
  restartDelay?: number;
  /** When the replication slot is in use e.g. by another service, this service will still continue to try to connect in case the other service stops. Delay is given in milliseconds, the default is 10s. */
  restartDelaySlotInUse?: number;
  /** Inbox message handlers or the outbox message sender that do not finish can block further messages from being processed/sent. The timeout (in milliseconds) ensures to continue with the next items. Default is 15s. */
  messageProcessingTimeout?: number;
}

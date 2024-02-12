import { ListenerConfig, ListenerSettings } from '../common/base-config';

export interface ReplicationConfig extends ListenerConfig {
  /** Replication listener specific configurations */
  settings: ReplicationListenerConfig;
}

export interface ReplicationListenerConfig extends ListenerSettings {
  /** The name of the used PostgreSQL publication */
  postgresPub: string;
  /** The name of the used PostgreSQL logical replication slot */
  postgresSlot: string;
  /** When there is a message processing error it restarts the logical replication subscription with a delay. This setting defines this delay in milliseconds. Default is 250ms. */
  restartDelayInMs?: number;
  /** When the replication slot is in use e.g. by another service, this service will still continue to try to connect in case the other service stops. Delay is given in milliseconds, the default is 10s. */
  restartDelaySlotInUseInMs?: number;
}

import { createService, ServiceConfig } from './local-replication-service';
import { OutboxMessage } from './models';

export type OutboxServiceConfig = ServiceConfig;

/**
 * Initialize the service to watch for outbox table inserts via logical replication.
 * @param config The configuration object with required values to connect to the WAL.
 * @param callback The callback is called to actually send the message through a message bus or other means.
 * @returns Functions for a clean shutdown and to help testing "outages" of the outbox service
 */
export const initializeOutboxService = (
  config: OutboxServiceConfig,
  callback: (message: OutboxMessage) => Promise<void>,
): [shutdown: { (): Promise<void> }] => createService(config, callback);

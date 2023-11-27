import { ErrorType } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { OutboxMessage } from '../common/message';
import { TransactionalOutboxInboxConfig } from '../replication/config';
import {
  TransactionalStrategies,
  createLogicalReplicationListener,
} from '../replication/logical-replication-listener';

export type OutboxConfig = TransactionalOutboxInboxConfig;

/**
 * Initialize the listener to watch for outbox table inserts via logical replication.
 * @param config The configuration object with required values to connect to the WAL.
 * @param sendMessage This function is called in which you should actually send the message through a message bus or other means.
 * @param logger A logger instance for logging trace up to error logs
 * @param strategies Strategies to provide custom logic for handling specific scenarios
 * @returns Functions for a clean shutdown and to help testing "outages" of the outbox listener
 */
export const initializeOutboxListener = (
  config: OutboxConfig,
  sendMessage: (message: OutboxMessage) => Promise<void>,
  logger: TransactionalLogger,
  strategies?: TransactionalStrategies,
): [shutdown: { (): Promise<void> }] => {
  const logErrors = async (
    _m: OutboxMessage,
    error: Error,
  ): Promise<ErrorType> => {
    logger.error(error, 'An error ocurred while handling an outbox message.');
    return 'transient_error';
  };
  strategies = strategies ?? {};
  return createLogicalReplicationListener(
    config,
    sendMessage,
    logErrors,
    logger,
    strategies,
  );
};

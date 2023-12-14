import { TransactionalLogger } from '../common/logger';
import { OutboxMessage } from '../common/message';
import { TransactionalOutboxInboxConfig } from '../replication/config';
import {
  TransactionalStrategies,
  createLogicalReplicationListener,
} from '../replication/logical-replication-listener';
import { defaultConcurrencyStrategy } from '../strategies/concurrency-strategy';
import { defaultListenerRestartTimeStrategy } from '../strategies/listener-restart-time-strategy';
import { defaultMessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';

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
  ): Promise<boolean> => {
    logger.error(error, 'An error ocurred while handling an outbox message.');
    return true;
  };
  return createLogicalReplicationListener(
    config,
    sendMessage,
    logErrors,
    logger,
    applyDefaultStrategies(strategies, config),
    'outbox',
  );
};

const applyDefaultStrategies = (
  strategies: TransactionalStrategies | undefined,
  config: OutboxConfig,
): Required<TransactionalStrategies> => ({
  concurrencyStrategy:
    strategies?.concurrencyStrategy ?? defaultConcurrencyStrategy(),
  messageProcessingTimeoutStrategy:
    strategies?.messageProcessingTimeoutStrategy ??
    defaultMessageProcessingTimeoutStrategy(config),
  listenerRestartTimeStrategy:
    strategies?.listenerRestartTimeStrategy ??
    defaultListenerRestartTimeStrategy(config),
});

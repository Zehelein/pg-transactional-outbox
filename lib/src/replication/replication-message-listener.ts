import { TransactionalLogger } from '../common/logger';
import { createErrorHandler } from '../handler/create-error-handler';
import { createMessageHandler } from '../handler/create-message-handler';
import { GeneralMessageHandler } from '../handler/general-message-handler';
import { TransactionalMessageHandler } from '../handler/transactional-message-handler';
import { defaultMessageProcessingDbClientStrategy } from '../strategies/message-processing-db-client-strategy';
import { defaultMessageProcessingTimeoutStrategy } from '../strategies/message-processing-timeout-strategy';
import { defaultMessageProcessingTransactionLevelStrategy } from '../strategies/message-processing-transaction-level-strategy';
import { defaultMessageRetryStrategy } from '../strategies/message-retry-strategy';
import { defaultPoisonousMessageRetryStrategy } from '../strategies/poisonous-message-retry-strategy';
import { ReplicationListenerConfig } from './config';
import { createLogicalReplicationListener } from './logical-replication-listener';
import { ReplicationMessageStrategies } from './replication-strategies';
import { defaultReplicationConcurrencyStrategy } from './strategies/concurrency-strategy';
import { defaultReplicationListenerRestartStrategy } from './strategies/listener-restart-strategy';

/**
 * Initialize the listener to watch for outbox or inbox table inserts via
 * PostgreSQL logical replication.
 * @param config The configuration object with required values to connect to the WAL.
 * @param messageHandlers A list of message handlers to handle specific messages or a single general message handler that handles all messages.
 * @param logger A logger instance for logging trace up to error logs
 * @param strategies Strategies to provide custom logic for handling specific scenarios
 * @returns Functions for a clean shutdown.
 */
export const initializeReplicationMessageListener = (
  config: ReplicationListenerConfig,
  messageHandlers: TransactionalMessageHandler[] | GeneralMessageHandler,
  logger: TransactionalLogger,
  strategies?: Partial<ReplicationMessageStrategies>,
): [shutdown: { (): Promise<void> }] => {
  const allStrategies = applyDefaultStrategies(strategies, config, logger);
  const messageHandler = createMessageHandler(
    messageHandlers,
    allStrategies,
    config,
    logger,
    'replication',
  );
  const errorHandler = createErrorHandler(
    messageHandlers,
    allStrategies,
    config,
    logger,
  );
  const [shutdown] = createLogicalReplicationListener(
    config,
    messageHandler,
    errorHandler,
    logger,
    allStrategies,
  );
  return [
    async () => {
      await Promise.all([
        allStrategies.messageProcessingDbClientStrategy?.shutdown(),
        shutdown(),
      ]);
    },
  ];
};

const applyDefaultStrategies = (
  strategies: Partial<ReplicationMessageStrategies> | undefined,
  config: ReplicationListenerConfig,
  logger: TransactionalLogger,
): ReplicationMessageStrategies => ({
  concurrencyStrategy:
    strategies?.concurrencyStrategy ?? defaultReplicationConcurrencyStrategy(),
  messageProcessingDbClientStrategy:
    strategies?.messageProcessingDbClientStrategy ??
    defaultMessageProcessingDbClientStrategy(config, logger),
  messageProcessingTimeoutStrategy:
    strategies?.messageProcessingTimeoutStrategy ??
    defaultMessageProcessingTimeoutStrategy(config),
  messageProcessingTransactionLevelStrategy:
    strategies?.messageProcessingTransactionLevelStrategy ??
    defaultMessageProcessingTransactionLevelStrategy(),
  messageRetryStrategy:
    strategies?.messageRetryStrategy ?? defaultMessageRetryStrategy(config),
  poisonousMessageRetryStrategy:
    strategies?.poisonousMessageRetryStrategy ??
    defaultPoisonousMessageRetryStrategy(config),
  listenerRestartStrategy:
    strategies?.listenerRestartStrategy ??
    defaultReplicationListenerRestartStrategy(config),
});

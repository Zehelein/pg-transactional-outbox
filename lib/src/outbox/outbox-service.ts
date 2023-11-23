import { ErrorType } from '../common/error';
import { TransactionalLogger } from '../common/logger';
import { OutboxMessage } from '../common/message';
import { ConcurrencyController } from '../concurrency-controller/concurrency-controller';
import { ServiceConfig } from '../replication/config';
import { createService } from '../replication/replication-service';

export type OutboxServiceConfig = ServiceConfig;

/**
 * Initialize the service to watch for outbox table inserts via logical replication.
 * @param config The configuration object with required values to connect to the WAL.
 * @param sendMessage This function is called in which you should actually send the message through a message bus or other means.
 * @param logger A logger instance for logging trace up to error logs
 * @param concurrencyController A controller that ensures specific concurrency guarantees. Defaults to the `createMutexConcurrencyController`.
 * @returns Functions for a clean shutdown and to help testing "outages" of the outbox service
 */
export const initializeOutboxService = (
  config: OutboxServiceConfig,
  sendMessage: (message: OutboxMessage) => Promise<void>,
  logger: TransactionalLogger,
  concurrencyController: ConcurrencyController,
): [shutdown: { (): Promise<void> }] => {
  const logErrors = async (
    _m: OutboxMessage,
    error: Error,
  ): Promise<ErrorType> => {
    logger.error(error, 'An error ocurred while handling an outbox message.');
    return 'transient_error';
  };

  return createService(
    config,
    sendMessage,
    logErrors,
    concurrencyController,
    logger,
  );
};

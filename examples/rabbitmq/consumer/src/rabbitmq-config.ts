import { BrokerConfig } from 'rascal';
import { Config } from './config';
import { MovieAggregateType, MovieCreatedEventType } from './receive-movie';

/**
 * Creates a rascal configuration on how to use and configure the RabbitMQ vhost
 * This is currently a "publish only" setup.
 * Check https://www.npmjs.com/package/rascal for the full description with all
 * the available options.
 */
export const getMessagingConfig = (config: Config): BrokerConfig => {
  const cfg: BrokerConfig = {
    vhosts: {
      [config.rmqVHost]: {
        assert: config.rmqVHost !== '/',
        connection: {
          protocol: config.rmqProtocol,
          hostname: config.rmqHost,
          user: config.rmqUser,
          password: config.rmqPassword,
          port: config.rmqPort,
          management: {
            hostname: config.rmqMgmtHost,
            port: config.rmqMgmtPort,
            protocol: config.rmqMgmtProtocol,
            options: {
              timeout: 10000,
            },
          },
          options: {
            heartbeat: 10,
            connection_timeout: 10000,
            channelMax: config.rmqChannelMax,
          },
          socketOptions: {
            timeout: 10000,
            clientProperties: {
              connection_name: 'inbox-processing',
            },
          },
        },
        exchanges: {
          event: {
            assert: true,
            type: 'topic',
          },
          retry: {
            assert: true,
            type: 'topic',
          },
          dead_letter: {
            assert: true,
            type: 'topic',
          },
        },
        queues: {
          [`consumer:dead_letter`]: {
            options: {
              arguments: {
                'x-queue-type': 'quorum',
              },
            },
          },
          [`consumer:${MovieAggregateType}:${MovieCreatedEventType}`]: {
            options: {
              arguments: {
                'x-dead-letter-exchange': 'dead_letter',
                'x-dead-letter-routing-key': `consumer.dead_letter`,
                'x-queue-type': 'quorum',
              },
            },
          },
        },
        bindings: {
          [`consumer:dead_letter`]: {
            source: 'dead_letter',
            bindingKey: `consumer.dead_letter`,
            destination: `consumer:dead_letter`,
          },
          [`consumer:${MovieAggregateType}:${MovieCreatedEventType}`]: {
            source: 'event',
            bindingKey: `producer.${MovieAggregateType}.${MovieCreatedEventType}`,
            destination: `consumer:${MovieAggregateType}:${MovieCreatedEventType}`,
          },
        },
        subscriptions: {
          [MovieCreatedEventType]: {
            queue: `consumer:${MovieAggregateType}:${MovieCreatedEventType}`,
            prefetch: 10,
          },
        },
      },
    },
  };
  return cfg;
};

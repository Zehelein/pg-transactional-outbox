import { BrokerConfig, PublicationConfig } from 'rascal';
import { MovieAggregateType, MovieCreatedMessageType } from './add-movies';
import { Config } from './config';

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
              connection_name: 'outbox-publishing',
            },
          },
        },
        exchanges: {
          event: {
            assert: true,
            type: 'topic',
          },
        },
        publications: {
          ...buildPublication(MovieAggregateType, MovieCreatedMessageType),
        },
      },
    },
  };
  return cfg;
};

/** Build a publication configuration - the message type is also used as the publish topic */
const buildPublication = (
  aggregateType: string,
  messageType: string,
): {
  [key: string]: PublicationConfig;
} => {
  return {
    [MovieCreatedMessageType]: {
      exchange: 'event',
      routingKey: `producer.${aggregateType}.${messageType}`,
    },
  };
};

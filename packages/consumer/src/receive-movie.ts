import { Pool, PoolClient } from 'pg';
import { Config } from './config';
import { InboxMessage, ackInbox, nackInbox } from './inbox';
import { executeTransaction } from './utils';
import { logger } from './logger';

export const MovieAggregateType = 'movie';
export const MovieCreatedEventType = 'movie_created';

interface PublishedMovie {
  id: number;
  title: string;
  description: string;
}

function isPublishedMovie(value: unknown): asserts value is PublishedMovie {
  if (!value || typeof value !== 'object') {
    throw new Error('Not an object');
  }
  const obj = value as PublishedMovie;
  if (
    typeof obj.id !== 'number' ||
    typeof obj.title !== 'string' ||
    typeof obj.description !== 'string'
  ) {
    throw new Error('Not a valid PublishedMovie');
  }
}

const insertPublishedMovie = async (
  { id, title, description }: PublishedMovie,
  dbClient: PoolClient,
) => {
  await dbClient.query(
    /*sql*/ `
    INSERT INTO public.published_movies
      (id, title, description)
      VALUES ($1, $2, $3)`,
    [id, title, description],
  );
};

export const initializePublishedMovieStorage = (
  config: Config,
): { (msg: InboxMessage): Promise<void> } => {
  const pool = new Pool({
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresLoginRole,
    password: config.postgresLoginRolePassword,
    database: config.postgresDatabase,
  });
  pool.on('error', (err) => {
    logger.error(err, 'PostgreSQL pool error');
  });
  return async (msg: InboxMessage): Promise<void> => {
    const { payload } = msg;
    isPublishedMovie(payload);
    const result = await executeTransaction(pool, async (client) => {
      await insertPublishedMovie(payload, client);
      await ackInbox(msg, client, config);
    });
    if (result instanceof Error) {
      logger.error(
        {
          ...payload,
          error: result,
        },
        'Could not store the published movie',
      );
      await executeTransaction(pool, async (client) => {
        await nackInbox(msg, client, config);
      });
    } else {
      logger.trace(payload, 'Inserted the published movie');
    }
  };
};

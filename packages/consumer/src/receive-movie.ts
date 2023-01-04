import { Pool, PoolClient } from 'pg';
import { Config } from './config';
import { InboxMessage, ackInbox, nackInbox } from './inbox';
import { executeTransaction } from './utils';

export const MovieAggregateType = 'movie';
export const MovieCreatedEventType = 'movie_created';

interface PublishedMovie {
  id: number;
  title: string;
  description: string;
}

function isPublishedMovie(value: unknown): asserts value is PublishedMovie {
  if (!value || typeof value !== 'object') throw new Error('Not an object');
  const obj = value as PublishedMovie;
  if (
    typeof obj.id !== 'number' ||
    typeof obj.title !== 'string' ||
    typeof obj.description !== 'string'
  )
    throw new Error('Not a valid PublishedMovie');
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

export const initializePublishedMovieStorage = (config: Config) => {
  const pool = new Pool({
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresLoginRole,
    password: config.postgresLoginRolePassword,
    database: config.postgresDatabase,
  });
  pool.on('error', (err) => {
    console.log('Error pool', err.message);
  });
  return async (msg: InboxMessage) => {
    const { payload } = msg;
    isPublishedMovie(payload);
    const result = await executeTransaction(pool, async (client) => {
      await insertPublishedMovie(payload, client);
      await ackInbox(msg, client, config);
    });
    if (result instanceof Error) {
      console.error(
        `Could not store the published movie "${payload.title}" with id "${payload.id}."`,
        result,
      );
      await executeTransaction(pool, async (client) => {
        await nackInbox(msg, client, config);
      });
    } else {
      console.log(
        `Inserted the published movie "${payload.title}" with id "${payload.id}."`,
      );
    }
  };
};

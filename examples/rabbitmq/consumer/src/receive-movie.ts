import { ClientBase } from 'pg';
import { InboxMessage } from '../../../../lib/src';
import { logger } from './logger';

export const MovieAggregateType = 'movie';
export const MovieCreatedEventType = 'movie_created';

interface PublishedMovie {
  id: number;
  title: string;
  description: string;
}

function assertPublishedMovie(value: unknown): asserts value is PublishedMovie {
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
  dbClient: ClientBase,
) => {
  await dbClient.query(
    /*sql*/ `
    INSERT INTO public.published_movies
      (id, title, description)
      VALUES ($1, $2, $3)`,
    [id, title, description],
  );
};

/**
 * Stores the published video in the database
 * @param message The inbox message that contains the movie details
 * @param client The database client
 * @throws Error if the message does not contain a valid movie.
 */
export const storePublishedMovie = async (
  message: InboxMessage,
  client: ClientBase,
): Promise<void> => {
  const { payload } = message;
  assertPublishedMovie(payload);
  await insertPublishedMovie(payload, client);
  logger.trace(payload, 'Inserted the published movie');
};

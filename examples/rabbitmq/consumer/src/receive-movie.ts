import {
  DatabaseClient,
  TransactionalLogger,
  TransactionalMessage,
} from 'pg-transactional-outbox';

export const MovieAggregateType = 'movie';
export const MovieCreatedMessageType = 'movie_created';

/**
 * Get the function to store published movies in the database
 * @param logger A logger instance for logging trace up to error logs
 * @returns The movie function to store published movies
 */
export const getStorePublishedMovie =
  (logger: TransactionalLogger) =>
  /**
   * Stores the published video in the database
   * @param message The inbox message that contains the movie details
   * @param client The database client
   * @throws Error if the message does not contain a valid movie.
   */
  async (
    message: TransactionalMessage,
    client: DatabaseClient,
  ): Promise<void> => {
    const { payload } = message;
    assertPublishedMovie(payload);
    logger.trace(payload, 'Started to insert the published movie');
    // You can test poisonous messages by changing 'Z' to e.g. '0'
    if (message.id.startsWith('Z')) {
      logger.fatal(message, `Crashing message with ID ${message.id}.`);
      process.exit(1);
    }
    await insertPublishedMovie(payload, client);
    logger.trace(payload, 'Inserted the published movie');
  };

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
  dbClient: DatabaseClient,
) => {
  await dbClient.query(
    /* sql */ `
    INSERT INTO public.published_movies
      (id, title, description)
      VALUES ($1, $2, $3)`,
    [id, title, description],
  );
};

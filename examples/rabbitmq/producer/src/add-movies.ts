import { Pool, PoolClient } from 'pg';
import {
  initializeOutboxMessageStore,
  executeTransaction,
} from 'pg-transactional-outbox';
import { Config, getOutboxConfig } from './config';
import { logger } from './logger';

export const MovieAggregateType = 'movie';
export const MovieCreatedEventType = 'movie_created';

const insertMovie = async (dbClient: PoolClient) => {
  const movieInsertedIdResult = await dbClient.query(/*sql*/ `
        INSERT INTO public.movies (title, description, actors, directors, studio)
        VALUES ('some movie', 'some description', ARRAY['Some Actor'], ARRAY['Some Director'], 'Some Studio')
        RETURNING id, title, description;
      `);
  if (movieInsertedIdResult.rowCount === 0) {
    throw new Error('Could not insert the movie.');
  }
  // Selecting a few properties to send as part of the event
  const createdMovie = movieInsertedIdResult.rows[0];
  logger.trace(createdMovie, 'Stored a movie');
  return createdMovie;
};

/**
 * A business logic function to add business data (movies) to the database and
 * register an outbox message event "movie_created" for every inserted movie.
 * It uses a one second timeout to insert a movie and an outbox message every
 * second.
 * @param config The configuration object with details on how to connect to the database with the login role.
 */
export const addMovies = async (config: Config): Promise<void> => {
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

  // Create the outbox storage function for the movie created event
  const storeOutboxMessage = initializeOutboxMessageStore(
    MovieAggregateType,
    MovieCreatedEventType,
    getOutboxConfig(config),
  );

  setInterval(async () => {
    try {
      await executeTransaction(pool, async (client) => {
        const payload = await insertMovie(client);
        await storeOutboxMessage(payload.id, payload, client);
        return payload;
      });
    } catch (error) {
      logger.error(error, 'Could not create a movie');
    }
  }, 3000);
};

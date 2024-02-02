import { Pool, PoolClient } from 'pg';
import {
  IsolationLevel,
  ListenerConfig,
  TransactionalLogger,
  TransactionalMessage,
  executeTransaction,
  initializeMessageStorage,
} from 'pg-transactional-outbox';
import { v4 as uuid } from 'uuid';
import { Config } from './config';

export const MovieAggregateType = 'movie';
export const MovieCreatedMessageType = 'movie_created';

/**
 * A business logic function to add business data (movies) to the database and
 * register an outbox message "movie_created" for every inserted movie.
 * It uses a small timeout to insert a movie and an outbox message every
 * second.
 * @param config The configuration object with details on how to connect to the database with the login role.
 * @param listenerConfig The outbox related configuration settings
 * @param logger A logger instance for logging trace up to error logs
 */
export const addMovies = async (
  config: Config,
  listenerConfig: ListenerConfig,
  logger: TransactionalLogger,
): Promise<NodeJS.Timeout> => {
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

  // Create the outbox storage function for the movie created event message
  const storeOutboxMessage = initializeMessageStorage(listenerConfig, logger);

  return setInterval(async () => {
    try {
      await executeTransaction(
        await pool.connect(),
        async (client) => {
          const payload = await insertMovie(client, logger);
          const message: TransactionalMessage = {
            id: uuid(),
            aggregateId: payload.id,
            aggregateType: MovieAggregateType,
            messageType: MovieCreatedMessageType,
            payload,
            createdAt: new Date().toISOString(),
          };
          await storeOutboxMessage(message, client);
          return payload;
        },
        IsolationLevel.ReadCommitted,
      );
    } catch (error) {
      logger.error(error, 'Could not create a movie');
    }
  }, 3000);
};

let index = 1;
const insertMovie = async (
  dbClient: PoolClient,
  logger: TransactionalLogger,
) => {
  const movieInsertedIdResult = await dbClient.query(/* sql*/ `
        INSERT INTO public.movies (title, description, actors, directors, studio)
        VALUES ('movie ${index++}', 'some description', ARRAY['Some Actor'], ARRAY['Some Director'], 'Some Studio')
        RETURNING id, title, description;
      `);
  if (movieInsertedIdResult.rowCount === 0) {
    throw new Error('Could not insert the movie.');
  }
  // Selecting a few properties to send as part of the message
  const createdMovie = movieInsertedIdResult.rows[0];
  logger.trace(createdMovie, 'Stored a movie');
  return createdMovie;
};

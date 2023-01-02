import { Config } from './config';
import { Client } from 'pg';
import { outboxMessageStore } from './outbox';

export const MovieAggregateType = 'movie';
export const MovieCreatedEventType = 'movie_created';

const insertMovie = async (dbClient: Client) => {
  const movieInsertedIdResult = await dbClient.query(/*sql*/ `
        INSERT INTO public.movies (title, description, actors, directors, studio)
        VALUES ('some movie', 'some description', ARRAY['Some Actor'], ARRAY['Some Director'], 'Some Studio')
        RETURNING id, title;
      `);
  if (movieInsertedIdResult.rowCount === 0) {
    throw new Error('Could not insert the movie.');
  }
  const { id, title } = movieInsertedIdResult.rows[0];
  console.log(`Stored movie with id ${id}`);
  return { id, title };
};

/**
 * A business logic function to add business data (movies) to the database and
 * register an outbox message event "movie_created" for every inserted movie.
 * It uses a one second timeout to insert a movie and an outbox message every
 * second.
 * @param config The configuration object with details on how to connect to the database with the login role.
 */
export const addMovies = async (config: Config) => {
  const dbClient = new Client({
    host: config.postgresHost,
    port: config.postgresPort,
    user: config.postgresLoginRole,
    password: config.postgresLoginRolePassword,
    database: config.postgresDatabase,
  });
  await dbClient.connect();

  // pre-configure the specific kind of outbox message to generate
  const storeOutboxMessage = outboxMessageStore(
    MovieAggregateType,
    MovieCreatedEventType,
    config,
  );

  setInterval(async () => {
    try {
      await dbClient.query('BEGIN');
      const payload = await insertMovie(dbClient);
      await storeOutboxMessage(payload.id, payload, dbClient);
      await dbClient.query('COMMIT');
    } catch (error) {
      await dbClient.query('ROLLBACK');
      console.log(`Error when inserting a movie and outbox message`, error);
    }
  }, 1000);
};

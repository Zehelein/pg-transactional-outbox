import path from 'path';
// Load the environment files from the local and parent .env file
import dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '.env') });
dotenv.config({ path: path.join(__dirname, '../.env'), override: true });
// eslint-disable-next-line prettier/prettier
import fs from 'node:fs/promises';
import { DatabaseSetupExporter } from 'pg-transactional-outbox';
import { getConfig, getDatabaseSetupConfig } from '../src/config';
import { getLogger } from '../src/logger';

const logger = getLogger();
const { createReplicationScript, createPollingScript } = DatabaseSetupExporter;

(async () => {
  try {
    logger.info(
      'Creating SQL setup scripts for the replication or polling based transactional outbox using the .env file',
    );
    const config = getConfig();
    const setupConfig = getDatabaseSetupConfig(config);

    const replicationSql = createReplicationScript(setupConfig);
    const replicationFile = './setup/example-create-replication-outbox.sql';
    await fs.writeFile(replicationFile, replicationSql);
    logger.info(`Created the ${replicationFile}`);

    const pollingSql = createPollingScript(setupConfig);
    const pollingFile = './setup/example-create-polling-outbox.sql';
    await fs.writeFile(pollingFile, pollingSql);
    logger.info(`Created the ${pollingFile}`);
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
})();

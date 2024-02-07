import path from 'path';
// Load the environment files from the local and parent .env file
import dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '.env') });
dotenv.config({ path: path.join(__dirname, '../.env'), override: true });
// eslint-disable-next-line prettier/prettier
import fs from 'node:fs/promises';
import { DatabaseSetupExporter } from 'pg-transactional-outbox';
import {
  getConfig,
  getPollingInboxConfig,
  getReplicationInboxConfig,
} from '../src/config';
import { getLogger } from '../src/logger';

const logger = getLogger();
const { createReplicationScript, createPollingScript } = DatabaseSetupExporter;

(async () => {
  try {
    logger.info(
      'Creating SQL setup scripts for the replication or polling based transactional inbox using the .env file',
    );
    const config = getConfig();

    const replicationConfig = getReplicationInboxConfig(config);
    const replicationSql = createReplicationScript(replicationConfig);
    const replicationFile = './setup/example-create-replication-inbox.sql';
    await fs.writeFile(replicationFile, replicationSql);
    logger.info(`Created the ${replicationFile}`);

    const pollingConfig = getPollingInboxConfig(config);
    const pollingSql = createPollingScript(pollingConfig);
    const pollingFile = './setup/example-create-polling-inbox.sql';
    await fs.writeFile(pollingFile, pollingSql);
    logger.info(`Created the ${pollingFile}`);
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
})();

import fs from 'node:fs/promises';
import { DatabaseSetupExporter } from 'pg-transactional-outbox';
import {
  getConfig,
  getPollingInboxConfig,
  getReplicationInboxConfig,
} from '../src/config';

const { createReplicationScript, createPollingScript } = DatabaseSetupExporter;

(async () => {
  try {
    console.log(
      'Creating SQL setup scripts for the replication or polling based approach',
    );
    const config = getConfig();

    const inboxReplConfig = getReplicationInboxConfig(config);
    const inboxReplSql = createReplicationScript(inboxReplConfig);
    const inboxReplFile = './create-inbox-replication.sql';
    await fs.writeFile(inboxReplFile, inboxReplSql);
    console.log(`Created the ${inboxReplFile}`);

    const outboxReplConfig = getReplicationOutboxConfig(config);
    const outboxReplSql = createReplicationScript(outboxReplConfig);
    const outboxReplFile = './create-outbox-replication.sql';
    await fs.writeFile(outboxReplFile, outboxReplSql);
    console.log(`Created the ${outboxReplFile}`);

    const inboxPollConfig = getPollingInboxConfig(config);
    const inboxPollSql = createPollingScript(inboxPollConfig);
    const inboxPollFile = './setup/example-create-polling-inbox.sql';
    await fs.writeFile(inboxPollFile, inboxPollSql);
    console.log(`Created the ${inboxPollFile}`);

    const outboxPollConfig = getPollingOutboxConfig(config);
    const outboxPollSql = createPollingScript(outboxPollConfig);
    const outboxPollFile = './setup/example-create-polling-outbox.sql';
    await fs.writeFile(outboxPollFile, outboxPollSql);
    console.log(`Created the ${outboxPollFile}`);
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
})();

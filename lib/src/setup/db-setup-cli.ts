#! /usr/bin/env node

import { writeFile } from 'node:fs/promises';
import { Interface, createInterface } from 'readline';
import {
  printInboxPollingListenerEnvVariables,
  printOutboxPollingListenerEnvVariables,
} from '../polling/config';
import {
  printInboxReplicationListenerEnvVariables,
  printOutboxReplicationListenerEnvVariables,
} from '../replication/config';
import {
  DatabasePollingSetupConfig,
  DatabaseReplicationSetupConfig,
} from './database-setup';
import { DatabaseSetupExporter } from './database-setup-exporter';
const { createPollingScript, createReplicationScript } = DatabaseSetupExporter;

/** Async way to ask a question from the CLI */
const input = (prompt: string, rli: Interface): Promise<string> => {
  return new Promise((callbackFn) => {
    rli.question(prompt, (userInput: string): void => {
      callbackFn(userInput?.trim());
    });
  });
};

/** Get a value from the command line with allowed values and a default value */
const getValueFromInput = async (
  rli: Interface,
  prompt: string,
  allowedAnswers?: string[],
  defaultValue?: string,
): Promise<string> => {
  let answer;
  do {
    const d = defaultValue ? ` (default: ${defaultValue})` : '';
    answer = await input(`\x1b[32m${prompt}\x1b[0m${d}\n> `, rli);
    if (defaultValue && !answer) {
      answer = defaultValue;
    }
  } while (allowedAnswers ? allowedAnswers.indexOf(answer) < 0 : !answer);
  return answer;
};

/** Get config values for the logical replication listener */
const logicalReplicationValues = async (
  outboxOrInbox: 'outbox' | 'inbox',
  rli: Interface,
) => {
  const replicationSlot = await getValueFromInput(
    rli,
    `What should the name of the replication slot for the ${outboxOrInbox} be?`,
    undefined,
    `transactional_${outboxOrInbox}_slot`,
  );
  const publication = await getValueFromInput(
    rli,
    `What should the name of the replication publication for the ${outboxOrInbox} be?`,
    undefined,
    `transactional_${outboxOrInbox}_publication`,
  );
  return { replicationSlot, publication };
};

/** Get config values for the polling listener */
const pollingValues = async (
  outboxOrInbox: 'outbox' | 'inbox',
  schema: string,
  rli: Interface,
) => {
  const nextMessagesName = await getValueFromInput(
    rli,
    `What should the name of the ${outboxOrInbox} polling function be?`,
    undefined,
    `next_${outboxOrInbox}_messages`,
  );
  const nextMessagesSchema = await getValueFromInput(
    rli,
    `What should the name of the database schema for the "${nextMessagesName}" function be?`,
    undefined,
    schema,
  );
  return { nextMessagesName, nextMessagesSchema };
};

/** Execute the CLI */
export const dbSetupCli = async (): Promise<void> => {
  const rli = createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const getValue = (
    prompt: string,
    allowedAnswers?: string[],
    defaultValue?: string,
  ) => getValueFromInput(rli, prompt, allowedAnswers, defaultValue);

  const listener = await getValue(
    'What type of listener should be created?\n- Logical Replication (r)\n- Polling (p)',
    ['r', 'p'],
  );
  const database = await getValue('What is the name of your database?');
  const schema = await getValue(
    'What should the name of the database schema be?',
    undefined,
    'messaging',
  );
  const listenerRole = await getValue(
    'What is the name of your database role for the listener?',
    undefined,
    'messaging_listener',
  );
  const handlerRole = await getValue(
    'What is the name of your database role for the message handler?',
    undefined,
    listenerRole,
  );
  const outboxInboxBoth = await getValue(
    'What transactional types should be created?\n- outbox (o)\n- inbox (i)\n- both (b)',
    ['o', 'i', 'b'],
  );

  const buildReplicationSql = async (
    outboxOrInbox: 'outbox' | 'inbox',
    table: string,
    publication: string,
    replicationSlot: string,
    skipRoles = false,
  ) => {
    const config: DatabaseReplicationSetupConfig = {
      database,
      schema,
      table,
      listenerRole,
      handlerRole,
      outboxOrInbox,
      publication,
      replicationSlot,
    };
    return createReplicationScript(config, skipRoles);
  };

  const buildPollingSql = async (
    outboxOrInbox: 'outbox' | 'inbox',
    table: string,
    nextMessagesName: string,
    nextMessagesSchema: string,
    skipRoles = false,
  ) => {
    const config: DatabasePollingSetupConfig = {
      database,
      schema,
      table,
      listenerRole,
      handlerRole,
      outboxOrInbox,
      nextMessagesName,
      nextMessagesSchema,
    };
    return createPollingScript(config, skipRoles);
  };

  const envOverrides = { DB_SCHEMA: schema };

  let envConfig = '';
  let sqlOutput = '';
  if (outboxInboxBoth === 'o' || outboxInboxBoth === 'b') {
    const outboxOrInbox = 'outbox';
    const table = await getValue(
      'What name should the outbox table have?',
      undefined,
      outboxOrInbox,
    );
    if (listener === 'r') {
      const { publication, replicationSlot } = await logicalReplicationValues(
        outboxOrInbox,
        rli,
      );
      sqlOutput += await buildReplicationSql(
        outboxOrInbox,
        table,
        publication,
        replicationSlot,
      );
      envConfig += printOutboxReplicationListenerEnvVariables({
        ...envOverrides,
        TRX_OUTBOX_DB_TABLE: table,
        DB_PUBLICATION: publication,
        DB_REPLICATION_SLOT: replicationSlot,
      });
    } else {
      const { nextMessagesName, nextMessagesSchema } = await pollingValues(
        outboxOrInbox,
        schema,
        rli,
      );
      sqlOutput += await buildPollingSql(
        outboxOrInbox,
        nextMessagesName,
        nextMessagesSchema,
        table,
      );
      envConfig += printOutboxPollingListenerEnvVariables({
        ...envOverrides,
        TRX_OUTBOX_DB_TABLE: table,
        NEXT_MESSAGES_FUNCTION_SCHEMA: nextMessagesSchema,
        OUTBOX_NEXT_MESSAGES_FUNCTION_NAME: nextMessagesName,
      });
    }
  }
  if (outboxInboxBoth === 'i' || outboxInboxBoth === 'b') {
    const outboxOrInbox = 'inbox';
    const table = await getValue(
      'What name should the inbox table have?',
      undefined,
      outboxOrInbox,
    );
    if (listener === 'r') {
      const { publication, replicationSlot } = await logicalReplicationValues(
        outboxOrInbox,
        rli,
      );
      sqlOutput += await buildReplicationSql(
        outboxOrInbox,
        table,
        publication,
        replicationSlot,
        outboxInboxBoth === 'b',
      );
      envConfig += printInboxReplicationListenerEnvVariables({
        ...envOverrides,
        TRX_INBOX_DB_TABLE: table,
        DB_PUBLICATION: publication,
        DB_REPLICATION_SLOT: replicationSlot,
      });
    } else {
      const { nextMessagesName, nextMessagesSchema } = await pollingValues(
        outboxOrInbox,
        schema,
        rli,
      );
      sqlOutput += await buildPollingSql(
        outboxOrInbox,
        table,
        nextMessagesName,
        nextMessagesSchema,
        outboxInboxBoth === 'b',
      );
      envConfig += printInboxPollingListenerEnvVariables({
        ...envOverrides,
        TRX_INBOX_DB_TABLE: table,
        NEXT_MESSAGES_FUNCTION_SCHEMA: nextMessagesSchema,
        INBOX_NEXT_MESSAGES_FUNCTION_NAME: nextMessagesName,
      });
    }
  }
  const filename = await getValue(
    'What should the filename without extension for the SQL script (*.sql) and the config (*.env) be?',
    undefined,
    'transactional',
  );
  await writeFile(`${filename}.sql`, sqlOutput);

  // Write the .env File
  envConfig = `# Select the variables that you want to adjust and copy them to your .ENV file/store
# You can leave/skip the config variables that are already fine.
# The defaults will be applied automatically for them.
# If you use both outbox and inbox you can use the shared "TRX_***" variables or
# the outbox and inbox specific "TRX_OUTBOX_***" and "TRX_INBOX_***" ones.

${sortEnv(envConfig)}`;
  await writeFile(`${filename}.env`, envConfig);

  console.log(`File \x1b[92m${filename}\x1b[0m successfully created.`);
  rli.close();
};

const sortEnv = (envConfig: string) => {
  const lines = envConfig.split('\n');
  const genericLines = lines.filter(
    (l, i, all) =>
      !l.startsWith('TRX_INBOX_') &&
      !l.startsWith('TRX_OUTBOX_') &&
      !l.startsWith('#') &&
      i === all.indexOf(l),
  );
  const outboxLines = lines.filter((l) => l.startsWith('TRX_OUTBOX_'));
  const inboxLines = lines.filter((l) => l.startsWith('TRX_INBOX_'));
  return `# General settings for both outbox and inbox
${genericLines.join('\n')}
# Outbox specific settings - overrides the general settings
${outboxLines.join('\n')}
# Inbox specific settings - overrides the general settings
${inboxLines.join('\n')}`;
};

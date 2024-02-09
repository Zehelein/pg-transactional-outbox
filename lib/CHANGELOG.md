# Change Log

All notable changes to the pg-transactional-outbox library will be documented in
this file.

## [0.5.0] - 2024-02-01

### Added

- Support for a polling subscriber was added as an alternative to the logical
  replication listener.
- The following fields have to be added to the inbox table:

  ```sql
  ALTER TABLE public.inbox ADD COLUMN segment TEXT;
  ALTER TABLE public.inbox ADD COLUMN locked_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0);
  ALTER TABLE public.inbox ADD COLUMN concurrency TEXT NOT NULL DEFAULT 'sequential';
  ALTER TABLE public.inbox ADD COLUMN abandoned_at TIMESTAMPTZ;
  ALTER TABLE public.inbox ADD CONSTRAINT inbox_concurrency_check
    CHECK (concurrency IN ('sequential', 'parallel'));

  GRANT UPDATE (started_attempts, finished_attempts, processed_at, abandoned_at, locked_until) ON public.inbox TO db_inbox_handler;
  ```

- These fields must also be added to the outbox table:

  ```sql
  ALTER TABLE public.outbox ADD COLUMN segment TEXT;
  ALTER TABLE public.outbox ADD COLUMN locked_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0);
  ALTER TABLE public.outbox ADD COLUMN concurrency TEXT NOT NULL DEFAULT 'sequential';
  ALTER TABLE public.outbox ADD COLUMN abandoned_at TIMESTAMPTZ;
  ALTER TABLE public.outbox ADD CONSTRAINT outbox_concurrency_check
    CHECK (concurrency IN ('sequential', 'parallel'));

  GRANT UPDATE (started_attempts, finished_attempts, processed_at, abandoned_at, locked_until) ON public.outbox TO db_outbox_handler;
  ```

- Added automatic message cleanup. Please check the settings in the
  configuration to activate it and configure the times in seconds after which
  processed, abandoned, and in general messages should be deleted.

### Changed

- Aligned the outbox message handling logic to the inbox table. This allows to
  include maximum retries and poisonous message handling but slightly increases
  the time to send outbox messages.
- The following fields are now required (also) on the outbox table:
  ```sql
  ALTER TABLE public.outbox ADD COLUMN started_attempts smallint NOT NULL DEFAULT 0;
  ALTER TABLE public.outbox ADD COLUMN finished_attempts smallint NOT NULL DEFAULT 0;
  ALTER TABLE public.outbox ADD COLUMN processed_at TIMESTAMPTZ;
  ```
- Function names were renamed to not include "inbox" or "outbox" specifically
  anymore as both use the same underlying concept now. Generally the methods and
  type names reflect now more their roles:
  - "replication" is used for the logical replication based listener approach
  - "polling" on the other hand is used now for the polling listener approach
  - "handler" is used for code that is handling the outbox or inbox message
- To get (close) to the prior outbox handling logic you can set the settings
  fields `enableMaxAttemptsProtection` and `enablePoisonousMessageProtection` to
  `false`.

## [0.4.0] - 2024-02-01

### Changed

- BREAKING CHANGE: the outbox and inbox listeners accept now a logger instance
  to not depend on a global logger so custom names and settings can be provided
  to the outbox and inbox listener.
- BREAKING CHANGE: changed the term "service" to "listener" in multiple places
  to more accurately reflect what the code does.
- BREAKING CHANGE: renamed "attempts" to "finished_attempts" and added the
  column "started_attempts" in the transactional inbox. This was done to
  implement the poisonous message handling. Please run the following command to
  update your database (adjust the namespace to yours):

  ```sql
  ALTER TABLE public.inbox RENAME COLUMN attempts TO finished_attempts;
  ALTER TABLE public.inbox ADD COLUMN started_attempts smallint NOT NULL DEFAULT 0;
  GRANT UPDATE (started_attempts, finished_attempts, processed_at) ON public.inbox TO db_inbox_handler;
  ```

### Added

- Added an optional `strategies` object to the inbox and outbox listeners. It
  allows you to optionally define fine granular strategies on how to define
  specific logic around handling inbox and outbox messages.
- To manage the concurrency of message processing on a granular level, a
  concurrency manager can now be provided as part of the strategies for the
  outbox and inbox listener. The default will use a mutex to guarantee
  sequential message processing. There are the following pre-build ones but you
  can also write your own:
  - `createMutexConcurrencyController` - this controller guarantees sequential
    message processing.
  - `createFullConcurrencyController` - this controller allows the parallel
    processing of messages without guarantees on the processing order.
  - `createSemaphoreConcurrencyController` - this controller allows the
    processing of messages in parallel up to a configurable number.
  - `createDiscriminatingMutexConcurrencyController` - this controller enables
    sequential message processing based on a specified discriminator. This could
    be the message type or some other (calculated) value.
  - `createMultiConcurrencyController` - this is a combined concurrency
    controller. You can define for every message which from the above
    controllers the message should use.
- Messages are processed via message handlers as part of a database transaction.
  Some handlers may require a different database handler user. In this case, you
  can use the `messageProcessingDbClientStrategy` to return a database client
  from the desired database pool.
- The `messageProcessingTimeoutStrategy` allows you to define a message-based
  timeout on how long the message is allowed to be processed in milliseconds.
  This allows you to allow some more expensive messages to take longer while
  still keeping others on a short timeout. By default, it uses the configured
  messageProcessingTimeout or falls back to a 15-second timeout.
- The inbox listener lets you define the
  `messageProcessingTransactionLevelStrategy` per message. Some message
  processing logic may have higher isolation level requirements than for
  processing other messages. If no custom strategy is provided it uses the
  default database transaction level via `BEGIN`.
- Messages can fail when they are processed. The `messageRetryStrategy` allows
  you to define how often a message can be retried. And in case a message is
  (likely) a poisonous message that crashes the service you can use the
  `poisonousMessageRetryStrategy` to customize if and how often such a message
  can be retried.
- When the inbox or outbox listener fails due to an error it is restarted. The
  `listenerRestartStrategy` is used to define how long it should wait before it
  attempts to start again. It allows you to decide (based on the error) to log
  or track the caught error or try to resolve the underlying issue.

## [0.3.0] - 2023-10-23

### Changed

- BREAKING CHANGE: added support for additional "metadata" in outbox and inbox
  messages. A new database column `metadata JSONB` must be added to the inbox
  and outbox database table. This setting can hold any additional metadata e.g.
  routing information, message signature etc. Please run the following two
  commands to update your database (adjust the namespace to yours):
  ```sql
  ALTER TABLE public.inbox ADD COLUMN IF NOT EXISTS metadata JSONB;
  ALTER TABLE app_hidden.outbox ADD COLUMN IF NOT EXISTS metadata JSONB;
  ```
- BREAKING CHANGE: renamed "retries" to "attempts" for the transactional inbox
  to make it clear that "retries" include the initial attempt. Please run the
  following command to update your database (adjust the namespace to yours):
  ```sql
  ALTER TABLE app_public.inbox RENAME COLUMN retries TO attempts;
  ```

## [0.2.0] - 2023-10-26

### Changed

- BREAKING CHANGE: renamed "event type" to "message type" in the library and in
  the database columns. This was done to better transport the meaning that the
  transactional outbox and inbox can be used both for commands and events and
  not just for events. Please rename for your outbox and inbox table the
  `event_type` column to `message_type`. And in your code the message
  `eventType` field with `messageType`.

### Added

- The function `initializeGeneralOutboxMessageStorage` can now be used for a
  general outbox storage function that does not encapsulate the settings to
  store a specific message and aggregate type.

## [0.1.8] - 2023-09-22

### Changed

- Fixed an issue where "this" was not correctly bound when executing message
  handlers when they are provided as object methods.

## [0.1.7] - 2023-09-18

### Changed

- Improved published package contents to exclude unit test files.

## [0.1.6] - 2023-09-15

### Changed

- The logical replication service will now guarantee sequential message
  processing in the order how the messages were received. So far the messages
  were only started in the desired order but could finish in different order
  depending how long the message handler ran.

### Added

- Only one service can connect to the publication of a replication slot. When
  services are scaled, the first one will succeed to connect but the others will
  fail. There is now a new setting `restartDelaySlotInUse` to define the delay
  before trying to connect again if the replication slot is in use.

## [0.1.5] - 2023-09-11

### Added

- Debug log for replication start added. This way the actual start of the
  service and restarts can be tracked.

## [0.1.4] - 2023-05-15

### Changed

- Fixed an issue where messages were sometimes processed even after the maximum
  message retry for the inbox message was exceeded.

## [0.1.1 - 0.1.3] - 2023-01-28

### Changed

- Updated the readme files and referenced images.

## [0.1.0] - 2023-01-28

### Added

- Initial version of the library.

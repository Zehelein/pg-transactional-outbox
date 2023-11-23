# Change Log

All notable changes to the pg-transactional-outbox library will be documented in
this file.

## [0.4.0] - 2023-11-23

### Changed

- BREAKING CHANGE: the inbox and outbox services accept now a logger instance to
  not depend on a global logger so custom names and settings can be provided to
  the outbox and inbox service.
- BREAKING CHANGE: to manage the concurrency of message processing on a granular
  level, a concurrency manager is now required for the inbox and outbox service.
  There are pre-build ones:
  - `createMutexConcurrencyController` - this controller guarantees sequential
    message processing.
  - `createFullConcurrencyController` - this controller allows the parallel
    processing of messages without guarantees on the processing order.
  - `createDiscriminatingMutexConcurrencyController` - this controller enables
    sequential message processing based on a specified discriminator. This could
    be the message type or some other (calculated) value.
  - `createStrategyConcurrencyController` - this is a combined concurrency
    controller. You can define for every message by which from the above
    controllers the message should use.

## [0.3.0] - 2023-10-23

### Changed

- BREAKING CHANGE: added support for additional "metadata" in inbox and outbox
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
  transactional inbox and outbox can be used both for commands and events and
  not just for events. Please rename for your inbox and outbox table the
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

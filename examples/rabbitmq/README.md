# RabbitMQ-based transactional outbox and inbox

This example implements the transactional outbox and inbox pattern to send an
event message when a movie is created. It uses RabbitMQ for transporting the
message. RabbitMQ is an open-source message-broker software that implements the
Advanced Message Queuing Protocol (AMQP). In our example, it is used to exchange
"movie_created" messages between the producer and the consumer applications.

## Infrastructure

This service requires a PostgreSQL service to store the movies and to implement
the transactional outbox and inbox pattern. And a RabbitMQ instance to send the
messages. The root folder `./infra` contains a `docker-compose.yml` file to
create a local PostgreSQL and RabbitMQ instance for you. You can adjust the
settings - especially the ports.

## Setup

The producer and consumer services include a `.env.template` file in the project
root folders and in the setup folders (four in total). You need to create a copy
of them and store them as `.env` files. If you used the default infrastructure
setup the env values are already prepared.

A `setup:db` script is included in the producer and consumer service. Please run
it to set up the corresponding database, login roles, and table structure.

## Run

The package.json files include a "dev:watch" and "debug:watch" for development
and a "start" script that can be used after the project was built.

# Notes

The RabbitMQ connection is using [Rascal](https://github.com/onebeyond/rascal)
to send and receive messages. This library is built on top of
[amqplib](https://www.npmjs.com/package/amqplib) and adds a lot of options to
make message delivery more resilient.

This example uses a rather minimalistic configuration. If you want to use it for
production scenarios please check their documentation. You would at least need
to configure redeliveries and retries, maybe add message encryption and other
logic.

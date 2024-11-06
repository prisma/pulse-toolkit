# `@prisma/pulse-cdc-pg`

`@prisma/pulse-cdc-pg` provides PostgreSQL Change Data Capture (CDC) streaming primitives built on the popular [`node-pg`](https://www.npmjs.com/package/pg) package. This package uses [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) to stream changes in real-time from a PostreSQL database.

## Purpose

This package provides lower level utilities for working with PostgreSQL WAL data over a logical replication stream. Using it directly requires handling maintenance of publications and replication slots, fault recovery, acknowledgement of events, throughput monitoring, distribution of events, and durable storage of events. For most applications, we recommend using a higher level product like [Prisma Pulse](https://prisma.io/puse).

## Setup

### PostgreSQL Configuration

First, PostgreSQL must be configured to enable logical replication using the `wal_level=logical` configuration parameter. The specifics for setting this parameter will depend on your hosting provider. When starting PostgreSQL locally or using Docker, it can be specified on the command arguments using `postgres -c wal_level=logical`. See `docker-compose.yml` in the repository root as an example. The [Prisma Pulse documentation](https://www.prisma.io/docs/pulse/database-setup/general-database-instructions) also covers this setup in depth.

### PostgreSQL Logical Replication Setup

Next, it’s required to create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) and a [replication slot](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS).

A publication defines the set of tables that will be included in a subscription. Creating a publication will have no impact on the database.

```sql
CREATE PUBLICATION "my_publication" FOR ALL TABLES;
```

> ![NOTE]
> The publication `my_publication` will emit all changes for all the tables in the database when used by a replication slot. See the [Prisma Pulse documentation](https://www.prisma.io/docs/pulse/database-setup/general-database-instructions#creating-a-publication-slot) for examples of configuring publications in other ways.

A replication slot defines a single logical replication consumer. PostgreSQL will track the status of a replication slot and preserve change data until the consumer acknowledges the events. This enables the consumer to undergo maintenance or otherwise become unavailable without missing changes. It also means that PostgreSQL can consume significant additional disk space if the replication slot is created without a consumer. Be sure to begin consuming events soon after creating the replication slot!

It’s also necessary to specify the [logical decoding plugin](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-EXPLANATION-OUTPUT-PLUGINS) the replication slot will use. The logical decoding plugin decides the format of the data that will be made available to the consumer. `pgoutput` is an efficient binary encoding that is implemented in `@prisma/pulse-cdc-pg`. Other popular options include `test_decoding` and `wal2json`.

```sql
SELECT slot_name, lsn FROM pg_create_logical_replication_slot(
  'my_replication_slot', -- slot name
  'pgoutput' -- logical decoding plugin
);
```

### Logical Replication Consumer

An application can consume logical replication data composing `LogicalReplicationStream` and `PgOutputDecoderStream`.

`LogicalReplicationStream` extends from `ReadableStream`. It connects to the database using `node-pg` and uses the `START_REPLICATION` command to being streaming changes in real-time. The stream will emit `UInt8Array` instances containing encoded Write-Ahead Log (WAL) events.

`PgOutputDecoderStream` extends from `TransformStream`. It accepts `UInt8Array` instances containing Write-Ahead Log (WAL) events encoded with pgoutput. It emits objects matching `WalPgoutputMessage`. Each message contains the LSN, timestamp, and a `tag` that discriminates between different types of events.

```tsx
import {
  LogicalReplicationStream,
  PgOutputDecoderStream,
} from "@prisma/pulse-cdc-pg";

// create a replication stream connected to PostgreSQL
const replication = new LogicalReplicationStream({
  clientConfig: {
    connectionString: process.env.CONNECTION_STRING,
    application_name: "pulse-cdc-pg example",
  },
  protocolVersion: PgOutputDecoderStream.PROTOCOL_VERSION,
  publicationName: "my_publication",
  slotName: "my_replication_slot",
});

// pipe the replication data through the pgoutput decoder
const stream = replication.pipeThrough(new PgOutputDecoderStream());

// loop over events as they are emitted by the stream
console.info("listening for replication events...");
for await (const message of stream) {
  console.log("replication event", message);
  // be sure to acknowledge messages to allow the WAL to clean up
  replication.acknowledge(message.currentLsn);
}
```

Alternatively, the `PgOutputDecoder` class provides a utility for decoding pgoutput data outside of streaming.

## Replication Slot Maintenance

PostgreSQL provides a number of utility functions for tracking and performing maintenance on replication slots. It’s important to ensure replication slots are being actively consumed to prevent the Write-Ahead Log (WAL) from growing until it consumes all available disk space.

[`pg_stat_replication_slots`](https://pgpedia.info/p/pg_stat_replication_slots.html) lists all replication slots and their status. If there is an active consumer, the PID will be available here.

[`pg_replication_slot_advance`](https://pgpedia.info/p/pg_replication_slot_advance.html) can be used to manually acknowledge an LSN for a replication slot. This is useful if the consumer goes down or the backlog grows too quickly for the consumer.

[`pg_logical_slot_peek_changes`](https://pgpedia.info/p/pg_logical_slot_peek_changes.html) can be used to query the replication slot without consuming the events.

[`pg_drop_replication_slot`](https://pgpedia.info/p/pg_drop_replication_slot.html) drops a replication slot. This will also allow any backlog of Write-Ahead Log (WAL) preserved by the replication slot to be removed from disk as well.

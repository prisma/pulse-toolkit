# Prisma Pulse CDC

This package contains the CDC (change data capture) primitives that powers Prisma Pulse.

`LogicalReplicationStream` provides a `ReadableStream` wrapper for `START_REPLICATION` using node-pg. This stream will emit `Uint8Array` chunks as they are available from the database. The encoding of the data is dependent on the setting of the replication slot. Only pgoutput has been tested.

`LogicalReplicationStream` also exposes an `acknowledge` method for acknowledging the receipt of WAL messages. Calling `acknowledge` is critical to prevent the WAL from being retained by the database and consuming disk space. It's not necessary to acknowledge every message, though acknowledging a message will implicitly acknowledge all preceding messages.

`PgOutputDecoderStream` is a `TransformStream` for decoding `Uint8Array` pgoutput packets into JavaScript objects. It exposes a constant `PROTOCOL_VERSION` denoting the pgoutput version in use.

Backpressure is applied to the underlying socket implementation used by node-pg. Using a stream queueing strategy to prefetch and process data during idle time is recommended to improve performance.

```ts
import {
  LogicalReplicationStream,
  PgOutputDecoderStream,
} from "@prisma/pulse-cdc";
import { Client } from "pg";

// create a LogicalReplicationStream to read read from the WAL
const logical = new LogicalReplicationStream({
  clientConfig: {
    /** connection details **/
  },
  protocolVersion: PgOutputDecoderStream.PROTOCOL_VERSION,
  // CREATE PUBLICATION "my_publication" FOR ALL TABLES
  publicationName: "my_publication",
  // SELECT lsn FROM pg_create_logical_replication_slot('my_replication_slot', 'pgoutput')
  slotName: "my_replication_slot",
});

// create a PgOutputDecoderStream to decode the WAL data
const pgoutput = new PgOutputDecoderStream();

// pipe the logical replication stream through the decoder
const stream = logical.pipeThrough(pgoutput);

// read events using `for await` or `ReadableStream` methods
for await (const message of stream) {
  console.log("received", message);
  // make sure to acknowledge!
  await logical.acknowledge(message.lsn);
}
```

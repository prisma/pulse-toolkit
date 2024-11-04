import {
  LogicalReplicationStream,
  PgOutputDecoderStream,
} from "@prisma/pulse-cdc";
//! pg doesn't support ESM and requires a default export
import pg from "pg";

const CONNECTION_STRING =
  process.env["DATABASE_URL"]! ??
  "postgresql://postgres:password@localhost:34944/pulse?schema=public";
const PUBLICATION_NAME = "my_publication";
const SLOT_NAME = "my_replication_slot";

const client = new pg.Client(CONNECTION_STRING);
try {
  await client.connect();
  // create publication
  console.info("creating publication...");
  await client.query(`DROP PUBLICATION IF EXISTS "${PUBLICATION_NAME}"`);
  await client.query(`CREATE PUBLICATION "${PUBLICATION_NAME}" FOR ALL TABLES`);
  console.info("created publication");
  // create replication slot
  console.info("creating replication slot...");
  await client.query(
    "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1",
    [SLOT_NAME],
  );
  const result = await client.query(
    "SELECT lsn FROM pg_create_logical_replication_slot($1, 'pgoutput')",
    [SLOT_NAME],
  );
  console.info("created replication slot", result.rows.at(0));
} finally {
  await client.end();
}

const replication = new LogicalReplicationStream({
  clientConfig: {
    connectionString: CONNECTION_STRING,
    application_name: "pulse-cdc script",
  },
  protocolVersion: PgOutputDecoderStream.PROTOCOL_VERSION,
  publicationName: "my_publication",
  slotName: "my_replication_slot",
});
const stream = replication.pipeThrough(new PgOutputDecoderStream());
console.info("listening for replication events...");
for await (const message of stream) {
  console.log("replication event", message);
  replication.acknowledge(message.currentLsn);
}

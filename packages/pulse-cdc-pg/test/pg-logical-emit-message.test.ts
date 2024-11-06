import { Client } from "pg";
import { assert, beforeAll, expect, test, vi } from "vitest";
import { LogicalReplicationStream, PgOutputDecoderStream } from "../src/mod.js";
import { PgReplicationUtils } from "./test-utils.js";

const CONNECTION_STRING =
  process.env["DATABASE_URL"]! ??
  "postgresql://postgres:password@localhost:34944/pulse?schema=public";
const PUBLICATION = `test_${crypto.randomUUID().replace(/-/g, "")}`;
const SLOT = `test_${crypto.randomUUID().replace(/-/g, "")}`;

beforeAll(async () => {
  const client = new Client({ connectionString: CONNECTION_STRING });
  try {
    await client.connect();
    const utils = new PgReplicationUtils(client);
    await utils.createPublication(PUBLICATION);
    await utils.createReplicationSlot(SLOT);
  } finally {
    await client.end();
  }

  return async () => {
    const client = new Client({ connectionString: CONNECTION_STRING });
    try {
      await client.connect();
      const utils = new PgReplicationUtils(client);
      await utils.dropReplicationSlot(SLOT);
      await utils.dropPublication(PUBLICATION);
    } finally {
      await client.end();
    }
  };
});

test("supports custom messages in WAL", async () => {
  const client = new Client({ connectionString: CONNECTION_STRING });
  try {
    await client.connect();

    const stream = new LogicalReplicationStream({
      clientConfig: { connectionString: CONNECTION_STRING },
      includeCustomMessages: true,
      protocolVersion: PgOutputDecoderStream.PROTOCOL_VERSION,
      publicationName: PUBLICATION,
      slotName: SLOT,
    }).pipeThrough(new PgOutputDecoderStream());
    const reader = stream.getReader();
    try {
      const randomPrefix = crypto.randomUUID();
      const randomBytes = crypto.getRandomValues(new Uint8Array(10));
      const result = await client.query<{ lsn: string }>(
        // ! cast $2 as bytea or Postgres interprets it as encoded text
        `SELECT pg_logical_emit_message(false, $1, $2::bytea) AS lsn`,
        [randomPrefix, randomBytes],
      );
      const lsn = result.rows.at(0)?.lsn ?? "0/0";

      const message = await vi.waitUntil(async () => {
        const next = await reader.read();
        assert(!next.done);
        return next.value.type === "waldata" &&
          next.value.message.tag === "message" &&
          next.value.message.prefix === randomPrefix
          ? next.value.message
          : null;
      });
      expect(message).toEqual({
        content: randomBytes,
        flags: 0,
        messageLsn: lsn,
        prefix: randomPrefix,
        tag: "message",
        transactional: false,
      });
    } finally {
      await reader.cancel();
    }
  } finally {
    await client.end();
  }
});

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

test("decodes pgoutput messages (no replica identity)", async () => {
  const tableName = `test_${crypto.randomUUID().replace(/-/g, "")}`;
  const client = new Client({ connectionString: CONNECTION_STRING });
  try {
    await client.connect();
    await client.query(
      `CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, value TEXT NOT NULL);`,
    );

    const stream = new LogicalReplicationStream({
      clientConfig: { connectionString: CONNECTION_STRING },
      protocolVersion: PgOutputDecoderStream.PROTOCOL_VERSION,
      publicationName: PUBLICATION,
      slotName: SLOT,
    }).pipeThrough(new PgOutputDecoderStream());

    // expected relation data structure for all events
    const relation = expect.objectContaining({
      columns: [
        {
          flags: 1,
          name: "id",
          parser: expect.any(Function),
          typeMod: -1,
          typeName: null,
          typeOid: 23,
          typeSchema: null,
        },
        {
          flags: 0,
          name: "value",
          parser: expect.any(Function),
          typeMod: -1,
          typeName: null,
          typeOid: 25,
          typeSchema: null,
        },
      ],
      keyColumns: ["id"],
      name: tableName,
      oid: expect.any(Number),
      replicaIdentity: "default",
      schema: "public",
      tag: "relation",
    });

    const reader = stream.getReader();
    try {
      // should see a keepalive message first
      await expect(reader.read()).resolves.toEqual({
        done: false,
        value: {
          currentLsn: expect.stringMatching(/.*\/.*/),
          shouldRespond: false,
          systemTime: expect.any(Date),
          type: "keepalive",
        },
      });

      const randomValue = crypto.randomUUID();
      const randomValue2 = crypto.randomUUID();

      // insert some data
      await client.query(`INSERT INTO ${tableName} (value) VALUES ($1)`, [
        randomValue,
      ]);

      const inserted = await vi.waitUntil(async () => {
        const next = await reader.read();
        assert(!next.done);
        return next.value.type === "waldata" &&
          next.value.message.tag === "insert" &&
          next.value.message.relation.name === tableName
          ? next.value
          : null;
      });
      expect(inserted).toEqual({
        type: "waldata",
        currentLsn: expect.stringMatching(/.*\/.*/),
        messageLsn: expect.stringMatching(/.*\/.*/),
        systemTime: expect.any(Date),
        message: expect.objectContaining({
          tag: "insert",
          new: {
            id: 1,
            value: randomValue,
          },
          relation,
        }),
      });

      // update some data
      await client.query(`UPDATE ${tableName} SET value = $1`, [randomValue2]);

      const updated = await vi.waitUntil(async () => {
        const next = await reader.read();
        assert(!next.done);
        return next.value.type === "waldata" &&
          next.value.message.tag === "update" &&
          next.value.message.relation.name === tableName
          ? next.value
          : null;
      });
      expect(updated).toEqual({
        type: "waldata",
        currentLsn: expect.stringMatching(/.*\/.*/),
        messageLsn: expect.stringMatching(/.*\/.*/),
        systemTime: expect.any(Date),
        message: expect.objectContaining({
          tag: "update",
          new: {
            id: 1,
            value: randomValue2,
          },
          old: null, // old is only available with replica identity
          relation,
        }),
      });

      // delete some data
      await client.query(`DELETE FROM ${tableName}`);

      const deleted = await vi.waitUntil(async () => {
        const next = await reader.read();
        assert(!next.done);
        return next.value.type === "waldata" &&
          next.value.message.tag === "delete" &&
          next.value.message.relation.name === tableName
          ? next.value
          : null;
      });
      expect(deleted).toEqual({
        type: "waldata",
        currentLsn: expect.stringMatching(/.*\/.*/),
        messageLsn: expect.stringMatching(/.*\/.*/),
        systemTime: expect.any(Date),
        message: expect.objectContaining({
          tag: "delete",
          key: { id: expect.any(Number) },
          old: null, // old is only available with replica identity
          relation,
        }),
      });
    } finally {
      await reader.cancel();
    }
  } finally {
    await client.query(`DROP TABLE ${tableName};`);
    await client.end();
  }
});

test("decodes pgoutput messages (with replica identity)", async () => {
  const tableName = `test_${crypto.randomUUID().replace(/-/g, "")}`;
  const client = new Client({ connectionString: CONNECTION_STRING });
  try {
    await client.connect();
    await client.query(
      `CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, value TEXT NOT NULL);`,
    );
    // enable replica identity
    await client.query(`ALTER TABLE ${tableName} REPLICA IDENTITY FULL;`);

    const stream = new LogicalReplicationStream({
      clientConfig: { connectionString: CONNECTION_STRING },
      protocolVersion: PgOutputDecoderStream.PROTOCOL_VERSION,
      publicationName: PUBLICATION,
      slotName: SLOT,
    }).pipeThrough(new PgOutputDecoderStream());

    // expected relation data structure for all events
    const relation = expect.objectContaining({
      columns: [
        {
          flags: 1,
          name: "id",
          parser: expect.any(Function),
          typeMod: -1,
          typeName: null,
          typeOid: 23,
          typeSchema: null,
        },
        {
          flags: 1,
          name: "value",
          parser: expect.any(Function),
          typeMod: -1,
          typeName: null,
          typeOid: 25,
          typeSchema: null,
        },
      ],
      keyColumns: ["id", "value"],
      name: tableName,
      oid: expect.any(Number),
      replicaIdentity: "full",
      schema: "public",
      tag: "relation",
    });

    const reader = stream.getReader();
    try {
      // should see a keepalive message first
      await expect(reader.read()).resolves.toEqual({
        done: false,
        value: {
          currentLsn: expect.stringMatching(/.*\/.*/),
          shouldRespond: false,
          systemTime: expect.any(Date),
          type: "keepalive",
        },
      });

      const randomValue = crypto.randomUUID();
      const randomValue2 = crypto.randomUUID();

      // insert some data
      await client.query(`INSERT INTO ${tableName} (value) VALUES ($1)`, [
        randomValue,
      ]);

      const inserted = await vi.waitUntil(async () => {
        const next = await reader.read();
        assert(!next.done);
        return next.value.type === "waldata" &&
          next.value.message.tag === "insert" &&
          next.value.message.relation.name === tableName
          ? next.value
          : null;
      });
      expect(inserted).toEqual({
        type: "waldata",
        currentLsn: expect.stringMatching(/.*\/.*/),
        messageLsn: expect.stringMatching(/.*\/.*/),
        systemTime: expect.any(Date),
        message: expect.objectContaining({
          tag: "insert",
          new: {
            id: 1,
            value: randomValue,
          },
          relation,
        }),
      });

      // update some data
      await client.query(`UPDATE ${tableName} SET value = $1`, [randomValue2]);

      const updated = await vi.waitUntil(async () => {
        const next = await reader.read();
        assert(!next.done);
        return next.value.type === "waldata" &&
          next.value.message.tag === "update" &&
          next.value.message.relation.name === tableName
          ? next.value
          : null;
      });
      expect(updated).toEqual({
        type: "waldata",
        currentLsn: expect.stringMatching(/.*\/.*/),
        messageLsn: expect.stringMatching(/.*\/.*/),
        systemTime: expect.any(Date),
        message: expect.objectContaining({
          tag: "update",
          new: {
            id: 1,
            value: randomValue2,
          },
          old: {
            id: 1,
            value: randomValue,
          },
          relation,
        }),
      });

      // delete some data
      await client.query(`DELETE FROM ${tableName}`);

      const deleted = await vi.waitUntil(async () => {
        const next = await reader.read();
        assert(!next.done);
        return next.value.type === "waldata" &&
          next.value.message.tag === "delete" &&
          next.value.message.relation.name === tableName
          ? next.value
          : null;
      });
      expect(deleted).toEqual({
        type: "waldata",
        currentLsn: expect.stringMatching(/.*\/.*/),
        messageLsn: expect.stringMatching(/.*\/.*/),
        systemTime: expect.any(Date),
        message: expect.objectContaining({
          tag: "delete",
          key: null,
          old: {
            id: 1,
            value: randomValue2,
          },
          relation,
        }),
      });
    } finally {
      await reader.cancel();
    }
  } finally {
    await client.query(`DROP TABLE ${tableName};`);
    await client.end();
  }
});

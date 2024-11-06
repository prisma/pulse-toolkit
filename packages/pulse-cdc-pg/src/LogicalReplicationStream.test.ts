import { Client } from "pg";
import { beforeAll, expect, test } from "vitest";
import { PgReplicationUtils } from "../test/test-utils";
import { LogicalReplicationStream } from "./LogicalReplicationStream";

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

test("stream ends on reader cancel", async () => {
  const stream = new LogicalReplicationStream({
    clientConfig: { connectionString: CONNECTION_STRING },
    protocolVersion: 1,
    publicationName: PUBLICATION,
    slotName: SLOT,
  });

  const reader = stream.getReader();
  try {
    await expect(reader.read()).resolves.toEqual({
      done: false,
      value: expect.any(Uint8Array),
    });
    await reader.cancel();
    await expect(reader.read()).resolves.toEqual({
      done: true,
      value: undefined,
    });
  } finally {
    await reader.cancel();
  }
});

test("stream ends on stream dispose", async () => {
  const stream = new LogicalReplicationStream({
    clientConfig: { connectionString: CONNECTION_STRING },
    protocolVersion: 1,
    publicationName: PUBLICATION,
    slotName: SLOT,
  });

  const reader = stream.getReader();
  try {
    await expect(reader.read()).resolves.toEqual({
      done: false,
      value: expect.any(Uint8Array),
    });
    await stream.dispose();
    await expect(reader.read()).resolves.toEqual({
      done: true,
      value: undefined,
    });
  } finally {
    await reader.cancel();
  }
});

test("acknowledges", async () => {
  const stream = new LogicalReplicationStream({
    clientConfig: { connectionString: CONNECTION_STRING },
    protocolVersion: 1,
    publicationName: PUBLICATION,
    slotName: SLOT,
  });

  try {
    await stream.acknowledge("0/0");
  } finally {
    await stream.dispose();
  }
});

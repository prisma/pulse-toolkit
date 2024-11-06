import type { Writable } from "node:stream";
import pg, { type Client, type ClientConfig } from "pg";
import { CopyBothStreamQuery, both } from "pg-copy-streams";
import { AcknowledgePacket } from "./AcknowledgePacket.js";

interface LogicalReplicationStreamInit {
  clientConfig: ClientConfig;
  includeCustomMessages?: boolean;
  lsn?: string;
  protocolVersion: number;
  publicationName: string;
  slotName: string;
}

/**
 * Implementation of `ReadableStream` that reads logical replication data
 * using the `START_REPLICATION` command on the specified node-pg `Connection`.
 * Packets emitted from stream are enqueued as `Uint8Array` payloads to be
 * parsed by a downstream consumer.
 */
export class LogicalReplicationStream extends ReadableStream<Uint8Array> {
  readonly #client: Client;
  readonly #controller = new AbortController();
  readonly #stream: CopyBothStreamQuery;
  readonly #writer: Writable;

  constructor(
    init: LogicalReplicationStreamInit,
    queuingStrategy?: QueuingStrategy<Uint8Array>,
  ) {
    const {
      clientConfig,
      includeCustomMessages = false,
      lsn = "0/00000000",
      protocolVersion,
      publicationName,
      slotName,
    } = init;

    const sql = `START_REPLICATION SLOT "${slotName}" LOGICAL ${lsn} (proto_version '${protocolVersion}', publication_names '${publicationName}', messages '${includeCustomMessages}')`;
    const stream = both(sql, { alignOnCopyDataFrame: true });

    const client = new pg.Client({
      ...clientConfig,
      // @ts-expect-error this field is accepted but not in the types
      replication: "database",
    });

    super(
      {
        start: async (controller) => {
          client.once("end", () => {
            try {
              controller.close();
            } catch {
              // controller may be closed already
            }
          });

          await client.connect();

          // send the START_REPLICATION command and begin streaming
          client.query(stream);

          // pause the readable stream initially to enable manual mode
          stream.pause();

          // when data comes in, queue the chunk then pause again
          stream.on("data", (chunk: Buffer) => {
            // even though Buffer is a Uint8Array, it's not the same Uint8Array 😵
            controller.enqueue(new Uint8Array(chunk));
            stream.pause();
          });

          // other lifecycle handlers
          stream.on("end", () => {
            if (!this.#controller.signal.aborted) {
              controller.close();
            }
          });
          stream.on("close", () => {
            if (!this.#controller.signal.aborted) {
              controller.close();
            }
          });
          stream.on("error", (e) => {
            if (!this.#controller.signal.aborted) {
              if (e.message === "Connection terminated") {
                controller.close();
              } else {
                controller.error(e);
              }
            }
          });
        },
        pull() {
          // resume streaming to pull another packet
          // the "data" handler will pause again after a packet is read
          stream.resume();
        },
        cancel: () => this.dispose(),
      },
      queuingStrategy,
    );

    this.#client = client;
    this.#stream = stream;
    this.#writer = stream;
  }

  [Symbol.asyncDispose]() {
    return this.dispose();
  }

  /**
   * Sends an acknowledge packet to the database to move the LSN position of
   * the replication slot forward.
   *
   * This is critical for preventing the WAL from accumulating and consuming
   * all available disk space, which will lead to the database becoming unusable
   * until more disk is allocated.
   *
   * Acknowledging a LSN implicitly acknowledges all WAL events prior to that
   * LSN.
   *
   * @param lsn The LSN to acknowledge up to.
   */
  async acknowledge(lsn: string) {
    const { buffer } = new AcknowledgePacket(lsn);
    await new Promise<void>((resolve, reject) =>
      this.#writer.write(buffer, (error) =>
        error ? reject(error) : resolve(),
      ),
    );
  }

  async dispose() {
    // abort the controller to stop processing events above
    this.#controller.abort();

    // pause the stream in case there is a pending read
    this.#stream.pause();

    // ending the connection will send the appropriate close packet
    // this ensures PostgreSQL will reflect the connection's closed state
    // @ts-expect-error this field is exposed but not in the types
    const connection = this.#client.connection as Connection;
    connection.end();

    // it's still necessary to close the Client instance
    await this.#client.end();
  }
}

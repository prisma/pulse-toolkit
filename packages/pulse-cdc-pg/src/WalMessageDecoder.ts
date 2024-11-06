import { BinaryReader } from "./BinaryReader.js";
import { PgOutputProtocolError } from "./errors.js";

interface WalDataDecoder<T> {
  decode(buffer: Uint8Array): T;
}

/**
 * `WalMessageDecoder` decodes the output of PostgreSQL logical replication
 * messages.
 *
 * `WalMessageDecoder` decodes the outer WAL data envelope while delegating WAL
 * data messages to a specified decoder instance.
 *
 * @see {@link https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION PostgreSQL docs}
 */
export class WalMessageDecoder<T> {
  readonly #decoder: WalDataDecoder<T>;

  constructor(decoder: WalDataDecoder<T>) {
    this.#decoder = decoder;
  }

  decode(buffer: Uint8Array) {
    const reader = new BinaryReader(buffer);
    const type = reader.readUint8();
    switch (type) {
      case 0x6b /*k*/:
        return {
          type: "keepalive" as const,
          ...this.#keepalive(reader),
        };
      case 0x77 /*w*/:
        return {
          type: "waldata" as const,
          ...this.#waldata(reader),
        };
      default: {
        const char = String.fromCharCode(type);
        throw new PgOutputProtocolError(`unknown WAL message type "${char}"`);
      }
    }
  }

  /**
   * @see {@link https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-PRIMARY-KEEPALIVE-MESSAGE PostgreSQL docs}
   */
  #keepalive(reader: BinaryReader) {
    const currentLsn = reader.readLsn();
    const systemTime = reader.readTime();
    const shouldRespond = reader.readUint8() === 1;
    return {
      currentLsn,
      systemTime,
      shouldRespond,
    };
  }

  /**
   * @see {@link https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA PostgreSQL docs}
   */
  #waldata(reader: BinaryReader) {
    const messageLsn = reader.readLsn();
    const currentLsn = reader.readLsn();
    const systemTime = reader.readTime();
    const message = this.#decoder.decode(new Uint8Array(reader.remaining));
    return {
      currentLsn,
      message,
      messageLsn,
      systemTime,
    };
  }
}

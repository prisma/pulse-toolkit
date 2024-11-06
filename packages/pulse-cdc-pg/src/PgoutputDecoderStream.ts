import {
  PgoutputDecoder,
  type PgoutputDecoderInit,
} from "./PgoutputDecoder.js";
import { WalMessageDecoder } from "./WalMessageDecoder.js";

export type WalPgoutputMessage = ReturnType<
  WalMessageDecoder<ReturnType<PgoutputDecoder["decode"]>>["decode"]
>;

/**
 * Implementation of `TransformStream` that decodes pgoutput messages from
 * `Uint8Array` packets. Each packet written to the stream should contain a
 * single pgoutput message.
 *
 * @see {@link https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html PostgreSQL docs}
 */
export class PgOutputDecoderStream extends TransformStream<
  Uint8Array,
  WalPgoutputMessage
> {
  /**
   * The protocol version expected by this transform stream. Should be
   * specified in `START_REPLICATION`.
   */
  static get PROTOCOL_VERSION() {
    return PgoutputDecoder.PROTOCOL_VERSION;
  }

  constructor(init?: PgoutputDecoderInit) {
    const decoder = new WalMessageDecoder(new PgoutputDecoder(init));
    super({
      transform(bytes, controller) {
        const message = decoder.decode(bytes);
        controller.enqueue(message);
      },
    });
  }
}

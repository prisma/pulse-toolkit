/** PostgreSQL epoch at 2000-01-01T00:00:00Z in milliseconds. */
const EPOCH_MS = 946684800000;

/**
 * Standby status update packet. Used to inform the sender that the standby
 * has successfully processed up to the specified WAL position.
 *
 * @see {@link https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE PostgreSQL docs}
 */
export class AcknowledgePacket {
  readonly #lsn: string;

  constructor(lsn: string) {
    this.#lsn = lsn;
  }

  get buffer() {
    const slice = this.#lsn.split("/");
    let [upperWAL, lowerWAL] = [
      parseInt(slice[0]!, 16),
      parseInt(slice[1]!, 16),
    ];

    if (lowerWAL === 0xffffffff) {
      upperWAL = upperWAL + 1;
      lowerWAL = 0;
    } else {
      lowerWAL = lowerWAL + 1;
    }

    const response = new Uint8Array(34);
    const view = new DataView(response.buffer);

    // Byte1('r') identify message as a receiver status update
    view.setUint8(0, 0x72); // 'r'

    // Int64 last WAL Byte + 1 received and written to disk locally
    view.setUint32(1, upperWAL, false);
    view.setUint32(5, lowerWAL, false);

    // Int64 last WAL Byte + 1 flushed to disk in the standby
    view.setUint32(9, upperWAL, false);
    view.setUint32(13, lowerWAL, false);

    // Int64 last WAL Byte + 1 applied in the standby
    view.setUint32(17, upperWAL, false);
    view.setUint32(21, lowerWAL, false);

    // Int64 client's clock at the time of transmission
    // timestamp as microseconds since midnight 2000-01-01
    const now = BigInt(Date.now() - EPOCH_MS) * 1000n;
    view.setBigUint64(25, now, false);

    // 1 to request the server reply to this message immediately
    view.setUint8(33, 0);

    return response;
  }
}

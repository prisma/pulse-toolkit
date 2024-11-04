const textDecoder = new TextDecoder();

/**
 * Utility for reading PostgreSQL binary data from a `Uint8Array`.
 * Buffer position is automatically progressed as data is read.
 *
 * @see {@link https://www.postgresql.org/docs/current/protocol-message-types.html PostgreSQL docs}
 */
export class BinaryReader {
  #p = 0;
  readonly #view: DataView;

  constructor(buffer: Uint8Array) {
    this.#view = new DataView(
      buffer.buffer,
      buffer.byteOffset,
      buffer.byteLength,
    );
  }

  get remaining() {
    return this.#view.buffer.slice(this.#p);
  }

  decodeText(strBuf: Uint8Array) {
    return textDecoder.decode(strBuf);
  }

  read(n: number) {
    const end = this.#p + n;
    if (end > this.#view.byteLength) {
      throw new RangeError("Offset is outside the bounds of the DataView");
    }
    const slice = new Uint8Array(
      this.#view.buffer,
      this.#view.byteOffset,
      this.#view.byteLength,
    ).subarray(this.#p, end);
    this.#p += n;
    return slice;
  }

  readInt16() {
    const value = this.#view.getUint16(this.#p);
    this.#p += 2;
    return value;
  }

  readInt32() {
    const value = this.#view.getInt32(this.#p);
    this.#p += 4;
    return value;
  }

  readLengthEncodedString() {
    const length = this.readInt32();
    const buffer = this.read(length);
    return textDecoder.decode(buffer);
  }

  readLsn() {
    const h = this.readUint32();
    const l = this.readUint32();

    const h2 = h.toString(16).padStart(1, "0");
    const l2 = l.toString(16).padStart(1, "0");
    return `${h2}/${l2}`.toUpperCase();
  }

  readString() {
    const end = new Uint8Array(
      this.#view.buffer,
      this.#view.byteOffset,
      this.#view.byteLength,
    ).indexOf(0x00, this.#p);
    if (end < 0) {
      throw new RangeError("Offset is outside the bounds of the DataView");
    }

    const length = end - this.#p;
    const buffer = this.read(length);
    this.#p += 1; // offset for 0x00 terminator
    return this.decodeText(buffer);
  }

  readTime() {
    // (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY == 946684800000000n
    const micros = this.readUint64() + 946684800000000n;
    const millis = Number(micros / 1000n);
    return new Date(millis);
  }

  readUint8() {
    const value = this.#view.getUint8(this.#p);
    this.#p += 1;
    return value;
  }

  readUint32() {
    const value = this.#view.getUint32(this.#p);
    this.#p += 4;
    return value;
  }

  readUint64() {
    const value = this.#view.getBigUint64(this.#p);
    this.#p += 8;
    return value;
  }

  skip(n: number): this {
    this.#p += n;
    return this;
  }
}

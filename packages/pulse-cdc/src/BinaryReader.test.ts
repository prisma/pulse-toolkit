import { describe, expect, test } from "vitest";
import { BinaryReader } from "./BinaryReader";

describe("Int", () => {
  const buffer = Uint8Array.from({ length: 8 }, (_, i) => 0xff - i);

  test("readInt16", () => {
    const reader = new BinaryReader(buffer);
    expect(reader.readInt16()).toBe(0xfffe);
    expect(reader.readInt16()).toBe(0xfdfc);
    expect(reader.readInt16()).toBe(0xfbfa);
    expect(reader.readInt16()).toBe(0xf9f8);
    expect(() => reader.readInt16()).toThrow(
      /Offset is outside the bounds of the DataView/,
    );
  });

  test("readInt32", () => {
    const reader = new BinaryReader(buffer);
    expect(reader.readInt32()).toBe(-66052);
    expect(reader.readInt32()).toBe(-67438088);
    expect(() => reader.readInt32()).toThrow(
      /Offset is outside the bounds of the DataView/,
    );
  });
});

describe("Uint", () => {
  const buffer = Uint8Array.from({ length: 8 }, (_, i) => 0xff - i);

  test("readUint8", () => {
    const reader = new BinaryReader(buffer);
    expect(reader.readUint8()).toBe(0xff);
    expect(reader.readUint8()).toBe(0xfe);
    expect(reader.readUint8()).toBe(0xfd);
    expect(reader.readUint8()).toBe(0xfc);
    expect(reader.readUint8()).toBe(0xfb);
    expect(reader.readUint8()).toBe(0xfa);
    expect(reader.readUint8()).toBe(0xf9);
    expect(reader.readUint8()).toBe(0xf8);
    expect(() => reader.readUint8()).toThrow(
      /Offset is outside the bounds of the DataView/,
    );
  });

  test("readUint32", () => {
    const reader = new BinaryReader(buffer);
    expect(reader.readUint32()).toBe(0xfffefdfc);
    expect(reader.readUint32()).toBe(0xfbfaf9f8);
    expect(() => reader.readUint32()).toThrow(
      /Offset is outside the bounds of the DataView/,
    );
  });

  test("readUint64", () => {
    const reader = new BinaryReader(buffer);
    expect(reader.readUint64()).toBe(18446460386757245432n);
    expect(() => reader.readUint64()).toThrow(
      /Offset is outside the bounds of the DataView/,
    );
  });
});

test("read", () => {
  const buffer = Uint8Array.from({ length: 8 }, (_, i) => 0xff - i);

  const reader = new BinaryReader(buffer);
  expect(reader.read(4)).toEqual(buffer.subarray(0, 4));
  expect(reader.read(2)).toEqual(buffer.subarray(4, 6));
  expect(reader.read(1)).toEqual(buffer.subarray(6, 7));
  expect(reader.read(1)).toEqual(buffer.subarray(7, 8));
  expect(() => reader.read(1)).toThrow(
    /Offset is outside the bounds of the DataView/,
  );
});

test("readLengthEncodedString", () => {
  const encoder = new TextEncoder();
  const first = encoder.encode("first");
  const second = encoder.encode("second");
  const third = encoder.encode("third");
  const buffer = new Uint8Array([
    0x00,
    0x00,
    0x00,
    0x05,
    ...first,
    0x00,
    0x00,
    0x00,
    0x06,
    ...second,
    0x00,
    0x00,
    0x00,
    0x05,
    ...third,
  ]);

  const reader = new BinaryReader(buffer);
  expect(reader.readLengthEncodedString()).toBe("first");
  expect(reader.readLengthEncodedString()).toBe("second");
  expect(reader.readLengthEncodedString()).toBe("third");
  expect(() => reader.readLengthEncodedString()).toThrow(
    /Offset is outside the bounds of the DataView/,
  );
});

test("readString", () => {
  const encoder = new TextEncoder();
  const first = encoder.encode("first");
  const second = encoder.encode("second");
  const third = encoder.encode("third");
  const buffer = new Uint8Array([
    ...first,
    0x00,
    ...second,
    0x00,
    ...third,
    0x00,
  ]);

  const reader = new BinaryReader(buffer);
  expect(reader.readString()).toBe("first");
  expect(reader.readString()).toBe("second");
  expect(reader.readString()).toBe("third");
  expect(() => reader.readString()).toThrow(
    /Offset is outside the bounds of the DataView/,
  );
});

test("readTime", () => {
  const now = Date.now();
  const micros = BigInt(now * 1000);
  const buffer = new Uint8Array(8);
  const view = new DataView(buffer.buffer);
  view.setBigUint64(0, micros - 946684800000000n);

  const reader = new BinaryReader(buffer);
  expect(reader.readTime()).toEqual(new Date(now));
  expect(() => reader.readTime()).toThrow(
    /Offset is outside the bounds of the DataView/,
  );
});

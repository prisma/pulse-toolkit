import pg from "pg";
import { BinaryReader } from "./BinaryReader.js";
import { PgOutputProtocolError } from "./errors.js";
import type {
  PgoutputBegin,
  PgoutputCommit,
  PgoutputDelete,
  PgoutputInsert,
  PgoutputMessage,
  PgoutputMessages,
  PgoutputOrigin,
  PgoutputRelation,
  PgoutputTruncate,
  PgoutputType,
  PgoutputUpdate,
  RelationColumn,
} from "./messages.js";

export interface PgoutputDecoderInit {
  /**
   * PostgreSQL type parsers to use in decoding scalar types.
   * Defaults to `pg.types`.
   *
   * @default pg.types
   */
  types?: pg.CustomTypesConfig;
}

interface PgTypeDef {
  typeSchema: string;
  typeName: string;
}

/**
 * Decoder for pgoutput binary messages.
 *
 * @see {@link https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html | PostgreSQL docs}
 */
export class PgoutputDecoder {
  /**
   * The pgoutput protocol version expected by this decoder.
   */
  static get PROTOCOL_VERSION() {
    return 1 as const;
  }

  /** PostgreSQL type parsers to use in decoding scalar types. */
  readonly #types: pg.CustomTypesConfig;
  /** Cache of observed PostgreSQL relations keyed on the OID. */
  readonly #relationCache = new Map<number, PgoutputRelation>();
  /** Cache of observed PostgreSQL data types keyed on the OID. */
  readonly #typeCache = new Map<number, PgTypeDef>();

  constructor(init: PgoutputDecoderInit = {}) {
    this.#types = init.types ?? pg.types;
  }

  decode(buf: Uint8Array): PgoutputMessages {
    const reader = new BinaryReader(buf);
    const tag = reader.readUint8();

    switch (tag) {
      case 0x42 /*B*/:
        return this.#msgBegin(reader);
      case 0x4f /*O*/:
        return this.#msgOrigin(reader);
      case 0x59 /*Y*/:
        return this.#msgType(reader);
      case 0x52 /*R*/:
        return this.#msgRelation(reader);
      case 0x49 /*I*/:
        return this.#msgInsert(reader);
      case 0x55 /*U*/:
        return this.#msgUpdate(reader);
      case 0x44 /*D*/:
        return this.#msgDelete(reader);
      case 0x54 /*T*/:
        return this.#msgTruncate(reader);
      case 0x4d /*M*/:
        return this.#msgMessage(reader);
      case 0x43 /*C*/:
        return this.#msgCommit(reader);
      default: {
        const char = String.fromCharCode(tag);
        throw new PgOutputProtocolError(
          `unexpected pgoutput message tag "${char}"`,
        );
      }
    }
  }

  #getRelation(relid: number) {
    const relation = this.#relationCache.get(relid);
    if (!relation) {
      throw new PgOutputProtocolError(`missing relation ${relid}`);
    }
    return relation;
  }

  #msgBegin(reader: BinaryReader): PgoutputBegin {
    const commitLsn = reader.readLsn();
    const commitTime = reader.readTime();
    const xid = reader.readInt32();

    return {
      tag: "begin",
      commitLsn,
      commitTime,
      xid,
    };
  }

  #msgCommit(reader: BinaryReader): PgoutputCommit {
    const flags = reader.readUint8();
    const commitLsn = reader.readLsn(); // should be the same as begin message
    const commitEndLsn = reader.readLsn();
    const commitTime = reader.readTime();

    return {
      tag: "commit",
      flags,
      commitLsn,
      commitEndLsn,
      commitTime,
    };
  }

  #msgDelete(reader: BinaryReader): PgoutputDelete {
    const relid = reader.readInt32();
    const relation = this.#getRelation(relid);

    let key: Record<string, unknown> | null = null;
    let old: Record<string, unknown> | null = null;
    const subMsgKey = reader.readUint8();

    if (subMsgKey === 0x4b /*K*/) {
      key = this.#readKeyTuple(reader, relation);
    } else if (subMsgKey === 0x4f /*O*/) {
      old = this.#readTuple(reader, relation);
    } else {
      const char = String.fromCharCode(subMsgKey);
      throw new PgOutputProtocolError(`unknown submessage key ${char}`);
    }

    return {
      tag: "delete",
      relation,
      key,
      old,
    };
  }

  #msgInsert(reader: BinaryReader): PgoutputInsert {
    const relid = reader.readInt32();
    const relation = this.#getRelation(relid);

    reader.readUint8(); // consume the 'N' key

    const inserted = this.#readTuple(reader, relation);

    return {
      tag: "insert",
      relation,
      new: inserted,
    };
  }

  #msgMessage(reader: BinaryReader): PgoutputMessage {
    const flags = reader.readUint8();
    const messageLsn = reader.readLsn();
    const prefix = reader.readString();
    const length = reader.readInt32();
    const content = reader.read(length);

    return {
      tag: "message",
      flags,
      transactional: Boolean(flags & 0b1),
      messageLsn,
      prefix,
      content,
    };
  }

  #msgOrigin(reader: BinaryReader): PgoutputOrigin {
    const originLsn = reader.readLsn();
    const originName = reader.readString();

    return {
      tag: "origin",
      originLsn,
      originName,
    };
  }

  #msgRelation(reader: BinaryReader): PgoutputRelation {
    const oid = reader.readInt32();
    const schema = reader.readString();
    const name = reader.readString();
    const replicaIdentity = this.#readRelationReplicaIdentity(reader);
    const columns = Array.from({ length: reader.readInt16() }, () =>
      this.#readRelationColumn(reader),
    );
    const keyColumns = columns.filter((x) => x.flags & 0b1).map((x) => x.name);

    const msg: PgoutputRelation = {
      tag: "relation",
      oid,
      schema,
      name,
      replicaIdentity,
      columns,
      keyColumns,
    };

    this.#relationCache.set(oid, msg);

    return msg;
  }

  #msgTruncate(reader: BinaryReader): PgoutputTruncate {
    const nrels = reader.readInt32();
    const flags = reader.readUint8();
    const relations = Array.from({ length: nrels }, () => {
      const relid = reader.readInt32();
      return this.#getRelation(relid);
    });

    return {
      tag: "truncate",
      cascade: Boolean(flags & 0b1),
      restartIdentity: Boolean(flags & 0b10),
      relations,
    };
  }

  #msgType(reader: BinaryReader): PgoutputType {
    const typeOid = reader.readInt32();
    const typeSchema = reader.readString();
    const typeName = reader.readString();

    this.#typeCache.set(typeOid, { typeSchema, typeName });

    return {
      tag: "type",
      typeOid,
      typeSchema,
      typeName,
    };
  }

  #msgUpdate(reader: BinaryReader): PgoutputUpdate {
    const relid = reader.readInt32();
    const relation = this.#getRelation(relid);

    let key: Record<string, unknown> | null = null;
    let old: Record<string, unknown> | null = null;
    let new_: Record<string, unknown> | null = null;

    const subMsgKey = reader.readUint8();
    if (subMsgKey === 0x4b /*K*/) {
      key = this.#readKeyTuple(reader, relation);
      reader.readUint8(); // consume the 'N' key
      new_ = this.#readTuple(reader, relation);
    } else if (subMsgKey === 0x4f /*O*/) {
      old = this.#readTuple(reader, relation);
      reader.readUint8(); // consume the 'N' key
      new_ = this.#readTuple(reader, relation, old);
    } else if (subMsgKey === 0x4e /*N*/) {
      new_ = this.#readTuple(reader, relation);
    } else {
      const char = String.fromCharCode(subMsgKey);
      throw new PgOutputProtocolError(`unknown submessage key ${char}`);
    }

    return { tag: "update", relation, key, old, new: new_ };
  }

  #readKeyTuple(
    reader: BinaryReader,
    relation: PgoutputRelation,
  ): Record<string, unknown> {
    const tuple = this.#readTuple(reader, relation);

    const key: Record<string, unknown> = {};
    for (const k of relation.keyColumns) {
      // If value is `null`, then it is definitely not part of key,
      // because key cannot have nulls by documentation.
      // And if we got `null` while reading keyOnly tuple,
      // then it means that `null` is not actual value
      // but placeholder of non-key column.
      key[k] = tuple[k] === null ? undefined : tuple[k];
    }

    return key;
  }

  #readRelationColumn(reader: BinaryReader): RelationColumn {
    const flags = reader.readUint8();
    const name = reader.readString();
    const typeOid = reader.readInt32();
    const typeMod = reader.readInt32();

    return {
      flags,
      name,
      typeOid,
      typeMod,
      typeSchema: null,
      typeName: null,
      ...this.#typeCache.get(typeOid),
      parser: this.#types.getTypeParser(typeOid),
    };
  }

  #readRelationReplicaIdentity(reader: BinaryReader) {
    // https://www.postgresql.org/docs/14/catalog-pg-class.html
    const value = reader.readUint8();

    switch (value) {
      case 0x64 /*d*/:
        return "default";
      case 0x6e /*n*/:
        return "nothing";
      case 0x66 /*f*/:
        return "full";
      case 0x69 /*i*/:
        return "index";
      default: {
        const char = String.fromCharCode(value);
        throw new PgOutputProtocolError(
          `unknown replica identity value ${char}`,
        );
      }
    }
  }

  #readTuple(
    reader: BinaryReader,
    { columns }: PgoutputRelation,
    unchangedToastFallback?: Record<string, unknown> | null,
  ): Record<string, unknown> {
    const nfields = reader.readInt16();

    const tuple: Record<string, unknown> = {};
    for (let i = 0; i < nfields; i++) {
      const { name, parser } = columns[i]!;
      const kind = reader.readUint8();

      switch (kind) {
        case 0x62 /*b binary*/: {
          const bsize = reader.readInt32();
          const bval = reader.read(bsize);
          // TODO: should we clone the buffer here?
          tuple[name] = bval;
          break;
        }
        case 0x74 /*t text*/: {
          const valtext = reader.readLengthEncodedString();
          tuple[name] = parser(valtext);
          break;
        }
        case 0x6e /*n null*/:
          tuple[name] = null;
          break;
        case 0x75 /*u unchanged toast datum*/:
          tuple[name] = unchangedToastFallback?.[name];
          break;
        default: {
          const char = String.fromCharCode(kind);
          throw new PgOutputProtocolError(`unknown attribute kind ${char}`);
        }
      }
    }

    return tuple;
  }
}

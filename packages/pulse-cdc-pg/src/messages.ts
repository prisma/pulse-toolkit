import type { TypeParser } from "pg-types";

export type PgoutputMessages =
  | PgoutputBegin
  | PgoutputCommit
  | PgoutputDelete
  | PgoutputInsert
  | PgoutputMessage
  | PgoutputOrigin
  | PgoutputRelation
  | PgoutputTruncate
  | PgoutputType
  | PgoutputUpdate;

export interface PgoutputBegin {
  tag: "begin";
  commitLsn: string | null;
  commitTime: Date;
  xid: number;
}

export interface PgoutputCommit {
  tag: "commit";
  flags: number;
  commitLsn: string | null;
  commitEndLsn: string | null;
  commitTime: Date;
}

export interface PgoutputDelete {
  tag: "delete";
  relation: PgoutputRelation;
  key: Record<string, unknown> | null;
  old: Record<string, unknown> | null;
}

export interface PgoutputInsert {
  tag: "insert";
  relation: PgoutputRelation;
  new: Record<string, unknown>;
}

export interface PgoutputMessage {
  tag: "message";
  flags: number;
  transactional: boolean;
  messageLsn: string | null;
  prefix: string;
  content: Uint8Array;
}

export interface PgoutputOrigin {
  tag: "origin";
  originLsn: string | null;
  originName: string;
}

export interface PgoutputRelation {
  tag: "relation";
  oid: number;
  schema: string;
  name: string;
  replicaIdentity: "default" | "nothing" | "full" | "index";
  columns: RelationColumn[];
  keyColumns: string[];
}

export interface PgoutputTruncate {
  tag: "truncate";
  cascade: boolean;
  restartIdentity: boolean;
  relations: PgoutputRelation[];
}

export interface PgoutputType {
  tag: "type";
  typeOid: number;
  typeSchema: string;
  typeName: string;
}

export interface PgoutputUpdate {
  tag: "update";
  relation: PgoutputRelation;
  key: Record<string, unknown> | null;
  old: Record<string, unknown> | null;
  new: Record<string, unknown>;
}

export interface RelationColumn {
  name: string;
  flags: number;
  typeOid: number;
  typeMod: number;
  typeSchema: string | null;
  typeName: string | null;
  parser: TypeParser<string, unknown>;
}

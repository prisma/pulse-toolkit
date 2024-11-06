declare module "pg-copy-streams" {
  import { Duplex, DuplexOptions } from "node:stream";
  import { Submittable } from "pg";

  export function both(
    txt: string,
    options?: DuplexOptions & { alignOnCopyDataFrame?: boolean },
  ): CopyBothStreamQuery;
  export class CopyBothStreamQuery extends Duplex implements Submittable {
    text: string;
    rowCount: number;
    submit(connection: Connection): void;
  }
}

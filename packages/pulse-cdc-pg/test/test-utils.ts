import type { Client } from "pg";

export class PgReplicationUtils {
  readonly #client;

  constructor(client: Client) {
    this.#client = client;
  }

  async createPublication(name: string) {
    const exists = await this.#client.query<{ publicationExists: boolean }>(
      `SELECT 1 FROM pg_publication WHERE pubname = $1`,
      [name],
    );
    if (exists.rows.length === 0) {
      // this cannot be parameterized because it's an object name
      await this.#client.query(`CREATE PUBLICATION "${name}" FOR ALL TABLES`);
    }
  }

  async createReplicationSlot(name: string) {
    const exists = await this.#client.query<{ lsn: string }>(
      `SELECT restart_lsn AS lsn FROM pg_replication_slots WHERE slot_name = $1`,
      [name],
    );
    if (exists.rows.length === 0) {
      await this.#client.query<{ lsn: string }>(
        `SELECT lsn FROM pg_create_logical_replication_slot($1, 'pgoutput')`,
        [name],
      );
    }
  }

  async dropPublication(name: string) {
    // this cannot be parameterized because it's an object name
    await this.#client.query(`DROP PUBLICATION IF EXISTS "${name}"`);
  }

  async dropReplicationSlot(name: string) {
    await this.#client.query(
      `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`,
      [name],
    );
  }

  async getCurrentLSN() {
    const result = await this.#client.query<{ lsn: string }>(
      "SELECT pg_current_wal_lsn() AS lsn",
    );
    const first = result.rows.at(0);
    return first?.lsn ?? "0/0";
  }
}

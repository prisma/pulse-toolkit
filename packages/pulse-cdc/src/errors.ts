export class PgOutputProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PgOutputProtocolError";
  }
}

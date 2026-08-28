import type { DatabaseSync } from "node:sqlite";
import { SCHEMA_VERSION } from "../constants.ts";
import { DurableQMigrationError, DurableQSchemaVersionError } from "../errors.ts";
import { SCHEMA_SQL } from "./schema.ts";
import { transact } from "./database.ts";

const VERSION_KEY = "schema_version";

function readVersion(db: DatabaseSync): string | null {
  const row = db.prepare("SELECT value FROM _durableq_meta WHERE key = ?").get(VERSION_KEY) as
    { value: string } | undefined;
  return row?.value ?? null;
}

export function migrate(db: DatabaseSync): number {
  db.exec(`
    CREATE TABLE IF NOT EXISTS _durableq_meta (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    ) STRICT;
  `);

  return transact(db, () => {
    const raw = readVersion(db);
    if (raw === null) {
      db.exec(SCHEMA_SQL);
      db.prepare("INSERT INTO _durableq_meta (key, value) VALUES (?, ?)").run(
        VERSION_KEY,
        String(SCHEMA_VERSION),
      );
      return SCHEMA_VERSION;
    }
    if (!/^[0-9]+$/.test(raw)) {
      throw new DurableQSchemaVersionError(`malformed schema version: ${raw}`);
    }
    const version = Number(raw);
    if (version > SCHEMA_VERSION) {
      throw new DurableQSchemaVersionError(
        `database schema version ${version} is newer than this package (${SCHEMA_VERSION})`,
      );
    }
    if (version < SCHEMA_VERSION) {
      throw new DurableQMigrationError(
        `cannot automatically upgrade schema from ${version} to ${SCHEMA_VERSION}`,
      );
    }
    return version;
  });
}

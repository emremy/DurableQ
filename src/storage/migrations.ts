import type { DatabaseSync } from "node:sqlite";
import { SCHEMA_VERSION } from "../constants.ts";
import { DurableQMigrationError, DurableQSchemaVersionError } from "../errors.ts";
import { MIGRATIONS, SCHEMA_SQL } from "./schema.ts";
import { transact } from "./database.ts";
import type { TransactionCuts } from "./session.ts";

const VERSION_KEY = "schema_version";

if (SCHEMA_VERSION !== MIGRATIONS.length + 1) {
  throw new DurableQMigrationError("SCHEMA_VERSION does not match the migration list");
}

function readVersion(db: DatabaseSync): string | null {
  const row = db.prepare("SELECT value FROM _durableq_meta WHERE key = ?").get(VERSION_KEY) as
    { value: string } | undefined;
  return row?.value ?? null;
}

/**
 * Creates or upgrades the schema inside one BEGIN IMMEDIATE transaction, so concurrent
 * processes serialize on the write lock and an interrupted upgrade rolls back entirely.
 */
export function migrate(db: DatabaseSync, cuts?: TransactionCuts): number {
  // Fast path without the write lock: under heavy claim traffic from other processes a
  // BEGIN IMMEDIATE here could be starved past busyTimeoutMs although nothing needs doing.
  const hasMeta = db
    .prepare("SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = '_durableq_meta'")
    .get();
  if (hasMeta && readVersion(db) === String(SCHEMA_VERSION)) return SCHEMA_VERSION;

  db.exec(`
    CREATE TABLE IF NOT EXISTS _durableq_meta (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    ) STRICT;
  `);

  cuts?.beforeBegin?.();
  const result = transact(db, () => {
    cuts?.afterBegin?.();
    const raw = readVersion(db);
    let version: number;
    if (raw === null) {
      db.exec(SCHEMA_SQL);
      db.prepare("INSERT INTO _durableq_meta (key, value) VALUES (?, ?)").run(VERSION_KEY, "1");
      version = 1;
    } else {
      if (!/^[0-9]+$/.test(raw)) {
        throw new DurableQSchemaVersionError(`malformed schema version: ${raw}`);
      }
      version = Number(raw);
      if (version > SCHEMA_VERSION) {
        throw new DurableQSchemaVersionError(
          `database schema version ${version} is newer than this package (${SCHEMA_VERSION})`,
        );
      }
      if (version < 1) {
        throw new DurableQMigrationError(`unknown schema version ${version}`);
      }
    }
    if (version === SCHEMA_VERSION) return version;
    try {
      for (let v = version; v < SCHEMA_VERSION; v++) db.exec(MIGRATIONS[v - 1]!);
    } catch (cause) {
      throw new DurableQMigrationError(
        `failed to upgrade schema from ${version} to ${SCHEMA_VERSION}`,
        { cause },
      );
    }
    db.prepare("UPDATE _durableq_meta SET value = ? WHERE key = ?").run(
      String(SCHEMA_VERSION),
      VERSION_KEY,
    );
    cuts?.beforeCommit?.();
    return SCHEMA_VERSION;
  });
  cuts?.afterCommit?.();
  return result;
}

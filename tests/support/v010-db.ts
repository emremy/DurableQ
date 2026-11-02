import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";

const DUMP = readFileSync(new URL("../fixtures/v0.1.0/populated.sql", import.meta.url), "utf8");

/** Writes a database exactly as v0.1.0 left it (schema version 1, jobs in every status). */
export function createV010Database(path: string): void {
  const db = new DatabaseSync(path);
  try {
    db.exec("PRAGMA journal_mode = WAL");
    db.exec(DUMP);
  } finally {
    db.close();
  }
}

export function readRows(path: string, sql: string): Record<string, unknown>[] {
  const db = new DatabaseSync(path, { timeout: 5000 });
  try {
    return (db.prepare(sql).all() as Record<string, unknown>[]).map((r) => ({ ...r }));
  } finally {
    db.close();
  }
}

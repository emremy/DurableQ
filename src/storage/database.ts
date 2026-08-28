import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { DurableQStorageError } from "../errors.ts";

export function openDatabase(filePath: string, busyTimeoutMs: number): DatabaseSync {
  mkdirSync(dirname(filePath), { recursive: true });
  const db = new DatabaseSync(filePath, {
    timeout: busyTimeoutMs,
    defensive: true,
    allowExtension: false,
  });
  try {
    db.exec("PRAGMA journal_mode = WAL");
    db.exec("PRAGMA synchronous = FULL");
    db.exec("PRAGMA foreign_keys = ON");

    const journal = db.prepare("PRAGMA journal_mode").get() as { journal_mode: string };
    const sync = db.prepare("PRAGMA synchronous").get() as { synchronous: number };
    const fk = db.prepare("PRAGMA foreign_keys").get() as { foreign_keys: number };

    if (String(journal.journal_mode).toLowerCase() !== "wal") {
      throw new DurableQStorageError(
        `required journal_mode=WAL, got ${String(journal.journal_mode)}`,
      );
    }
    // SQLite: 0 OFF, 1 NORMAL, 2 FULL, 3 EXTRA
    if (Number(sync.synchronous) < 2) {
      throw new DurableQStorageError(`required synchronous=FULL, got ${String(sync.synchronous)}`);
    }
    if (Number(fk.foreign_keys) !== 1) {
      throw new DurableQStorageError("required foreign_keys=ON");
    }
  } catch (err) {
    db.close();
    throw err;
  }
  return db;
}

export function transact<T>(db: DatabaseSync, fn: () => T): T {
  db.exec("BEGIN IMMEDIATE");
  try {
    const result = fn();
    db.exec("COMMIT");
    return result;
  } catch (err) {
    try {
      db.exec("ROLLBACK");
    } catch {
      // connection may already be aborted
    }
    throw err;
  }
}

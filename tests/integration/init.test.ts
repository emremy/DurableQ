import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { Queue } from "../../src/queue.ts";
import { DurableQValidationError } from "../../src/errors.ts";
import { acquireRuntime, registrySize } from "../../src/storage/runtime-registry.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { configureFault } from "../support/test-control.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

test("INV-016/INV-020/INV-021 init WAL FULL and shared runtime", async () => {
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  const tmp = tempDbPath();
  const q1 = openQueue(tmp.path);
  const q2 = openQueue(tmp.path);
  try {
    await q1.count();
    await q2.count();
    assert.equal(registrySize(), 1);

    const db = new DatabaseSync(tmp.path, { timeout: 1000 });
    try {
      const journal = db.prepare("PRAGMA journal_mode").get() as { journal_mode: string };
      const sync = db.prepare("PRAGMA synchronous").get() as { synchronous: number };
      const fk = db.prepare("PRAGMA foreign_keys").get() as { foreign_keys: number };
      assert.equal(String(journal.journal_mode).toLowerCase(), "wal");
      assert.ok(Number(sync.synchronous) >= 2);
      assert.equal(Number(fk.foreign_keys), 1);
      const ver = db
        .prepare("SELECT value FROM _durableq_meta WHERE key = 'schema_version'")
        .get() as {
        value: string;
      };
      assert.equal(ver.value, "1");
    } finally {
      db.close();
    }
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q1.close();
    await q2.close();
    tmp.cleanup();
  }
});

test("INV-017 storage worker exit rejects outstanding RPCs", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.count();
    await configureFault(q, { point: "enqueue:before-begin", action: "exit" });
    await assert.rejects(() => q.add("x", { a: 1 }), /storage worker/i);
  } finally {
    await q.close().catch(() => undefined);
    tmp.cleanup();
  }
});

test("newer schema version is rejected", async () => {
  const tmp = tempDbPath();
  const rt = await acquireRuntime({ path: tmp.path, busyTimeoutMs: 50 });
  await rt.unref();
  const db = new DatabaseSync(tmp.path);
  db.prepare("UPDATE _durableq_meta SET value = '99' WHERE key = 'schema_version'").run();
  db.close();
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 });
  try {
    await assert.rejects(() => q.count(), /schema version/);
  } finally {
    await q.close().catch(() => undefined);
    tmp.cleanup();
  }
});

test("conflicting busyTimeoutMs on the same path is rejected", async () => {
  const tmp = tempDbPath();
  const q1 = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 });
  const q2 = new Queue("app", { path: tmp.path, busyTimeoutMs: 100 });
  try {
    await q1.count();
    await assert.rejects(() => q2.count(), DurableQValidationError);
    await assert.rejects(() => q2.count(), /busyTimeoutMs=50/);
  } finally {
    await q1.close();
    await q2.close().catch(() => undefined);
    tmp.cleanup();
  }
});

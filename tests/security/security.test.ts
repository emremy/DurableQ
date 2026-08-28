import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { DurableQSerializationError, DurableQValidationError } from "../../src/errors.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");

test("malicious strings are bound parameters not SQL", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const evil = "n'; DROP TABLE durableq_jobs; --";
    const data = JSON.parse('{"__proto__":1,"constructor":{"prototype":1}}') as {
      constructor: { prototype: number };
    };
    const { job } = await q.add(evil, data, { dedupeKey: evil });
    assert.equal(job.name, evil);
    assert.equal(await q.count(), 1);
    const again = await q.get(job.id);
    assert.equal(again?.name, evil);
    assert.equal(Object.hasOwn(Object.prototype, "polluted"), false);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("oversized payload rejected", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const big = "x".repeat(2_000_000);
    await assert.rejects(() => q.add("n", { big }));
    assert.equal(await q.count(), 0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("NUL bytes in names are rejected", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await assert.rejects(() => q.add("bad\0name", { a: 1 }), DurableQValidationError);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("malformed stored JSON is a classified error", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const { job } = await q.add("n", { a: 1 });
    await q.close();
    const db = new DatabaseSync(tmp.path);
    db.prepare("UPDATE durableq_jobs SET payload_json = 'not-json' WHERE id = ?").run(job.id);
    db.close();
    const q2 = openQueue(tmp.path);
    await assert.rejects(() => q2.get(job.id), DurableQSerializationError);
    await q2.close();
  } finally {
    tmp.cleanup();
  }
});

test("SQLite extensions stay disabled and package has no telemetry", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { a: 1 });
    await q.close();
    const db = new DatabaseSync(tmp.path, { allowExtension: false, defensive: true });
    assert.equal(typeof db.loadExtension, "function");
    assert.throws(() => {
      db.loadExtension("not-a-real-extension");
    });
    db.close();
  } finally {
    tmp.cleanup();
  }
  const pkg = readFileSync(join(root, "package.json"), "utf8");
  assert.equal(pkg.includes("telemetry"), false);
  const src = readFileSync(join(root, "src/index.ts"), "utf8");
  assert.equal(src.includes("http://"), false);
});

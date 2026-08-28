import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { DurableQSerializationError } from "../../src/errors.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { configureFault } from "../support/test-control.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

test("INV-001 enqueue is visible from a fresh connection", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const { job, deduplicated } = await q.add("email.send", { to: "a@b.c" }, { attempts: 3 });
    assert.equal(deduplicated, false);
    assert.equal(job.status, "queued");
    assert.equal(job.attemptCount, 0);
    assert.equal(job.leaseOwner, null);
    const db = new DatabaseSync(tmp.path);
    const row = db
      .prepare("SELECT status, payload_json FROM durableq_jobs WHERE id = ?")
      .get(job.id) as {
      status: string;
      payload_json: string;
    };
    db.close();
    assert.equal(row.status, "queued");
    assert.equal(JSON.parse(row.payload_json).to, "a@b.c");
    const again = await q.get(job.id);
    assert.equal(again?.id, job.id);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("INV-014 serialization failure inserts nothing", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await assert.rejects(
      () => q.add("x", { a: undefined as unknown as string }),
      DurableQSerializationError,
    );
    assert.equal(await q.count(), 0);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("INV-015 sequential dedupe first insert wins", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const a = await q.add("email.send", { n: 1 }, { dedupeKey: "k" });
    const b = await q.add("email.send", { n: 2 }, { dedupeKey: "k" });
    assert.equal(b.deduplicated, true);
    assert.equal(b.job.id, a.job.id);
    assert.deepEqual(b.job.data, { n: 1 });
    assert.equal(await q.count(), 1);
    const otherQ = openQueue(tmp.path, "other");
    const c = await otherQ.add("email.send", { n: 3 }, { dedupeKey: "k" });
    assert.equal(c.deduplicated, false);
    await otherQ.close();
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("D-004 dedupe reserved after cancel until clean", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const a = await q.add("n", { v: 1 }, { dedupeKey: "k" });
    const cancel = await q.cancel(a.job.id);
    assert.equal(cancel.cancelled, true);
    const b = await q.add("n", { v: 2 }, { dedupeKey: "k" });
    assert.equal(b.deduplicated, true);
    assert.equal(b.job.id, a.job.id);
    await q.clean({ status: "cancelled", before: Date.now() + 1000 });
    const c = await q.add("n", { v: 3 }, { dedupeKey: "k" });
    assert.equal(c.deduplicated, false);
    assert.notEqual(c.job.id, a.job.id);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("INV-023 cancel active is explicit non-success", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const { job } = await q.add("n", { v: 1 });
    const client = await q.storage();
    await client.request("claim", {
      nowMs: Date.now(),
      queue: "app",
      workerId: "w",
      names: ["n"],
      leaseDurationMs: 30_000,
    });
    const res = await q.cancel(job.id);
    assert.equal(res.cancelled, false);
    if (!res.cancelled) assert.equal(res.reason, "active");
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("INV-012 delayed jobs are not claimed early", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { v: 1 }, { delayMs: 60_000 });
    const client = await q.storage();
    const claimed = await client.request("claim", {
      nowMs: Date.now(),
      queue: "app",
      workerId: "w",
      names: ["n"],
      leaseDurationMs: 30_000,
    });
    assert.equal(claimed, null);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("D-006 enqueue F1-F3 do not persist; F4 persists", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    for (const point of [
      "enqueue:before-begin",
      "enqueue:after-begin",
      "enqueue:before-commit",
    ] as const) {
      await configureFault(q, { point, action: "throw" });
      await assert.rejects(() => q.add("n", { v: point }));
      await configureFault(q, { point, action: "none" });
    }
    assert.equal(await q.count(), 0);

    await configureFault(q, { point: "enqueue:after-commit", action: "throw" });
    await assert.rejects(() => q.add("n", { v: 1 }, { dedupeKey: "f4" }));
    await configureFault(q, { point: "enqueue:after-commit", action: "none" });
    const again = await q.add("n", { v: 99 }, { dedupeKey: "f4" });
    assert.equal(again.deduplicated, true);
    assert.deepEqual(again.job.data, { v: 1 });
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

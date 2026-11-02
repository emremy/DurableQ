import assert from "node:assert/strict";
import test from "node:test";
import {
  DurableQClosedError,
  DurableQSerializationError,
  DurableQValidationError,
} from "../../src/errors.ts";
import { MAX_BATCH_SIZE } from "../../src/constants.ts";
import { Queue } from "../../src/queue.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { configureFault } from "../support/test-control.ts";
import { ensureTestWorker } from "../support/install-test-worker.ts";

type Jobs = {
  "email.send": { input: { to: string }; output: null };
  "report.build": { input: { id: number }; output: null };
};

function typedQueue(path: string): Queue<Jobs> {
  ensureTestWorker();
  return new Queue<Jobs>("app", { path, busyTimeoutMs: 50 });
}

test("empty batch resolves to [] without touching storage", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    assert.deepEqual(await q.addMany([]), []);
    assert.equal(await q.count(), 0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
  await assert.rejects(() => q.addMany([]), DurableQClosedError);
});

test("single job and mixed names/options keep input order", async () => {
  const tmp = tempDbPath();
  const q = typedQueue(tmp.path);
  try {
    const [one] = await q.addMany([{ name: "email.send", data: { to: "a@example.com" } }]);
    assert.equal(one?.job.name, "email.send");
    assert.equal(one?.deduplicated, false);

    const out = await q.addMany([
      { name: "report.build", data: { id: 1 }, options: { priority: 9, attempts: 7 } },
      { name: "email.send", data: { to: "b@example.com" }, options: { delayMs: 5_000 } },
      {
        name: "report.build",
        data: { id: 2 },
        options: { backoff: { type: "fixed", delayMs: 10 }, attemptTimeoutMs: 1_000 },
      },
    ]);
    assert.deepEqual(
      out.map((r) => [r.job.name, r.job.data]),
      [
        ["report.build", { id: 1 }],
        ["email.send", { to: "b@example.com" }],
        ["report.build", { id: 2 }],
      ],
    );
    assert.equal(out[0]?.job.priority, 9);
    assert.equal(out[0]?.job.maxAttempts, 7);
    assert.ok((out[1]?.job.availableAt ?? 0) > (out[1]?.job.createdAt ?? 0));
    assert.deepEqual(out[2]?.job.backoff, { type: "fixed", delayMs: 10 });
    assert.equal(out[2]?.job.attemptTimeoutMs, 1_000);
    assert.equal(await q.count(), 4);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("large batch at the limit commits; over the limit is rejected", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const items = Array.from({ length: MAX_BATCH_SIZE }, (_, i) => ({ name: "n", data: { i } }));
    const out = await q.addMany(items);
    assert.equal(out.length, MAX_BATCH_SIZE);
    assert.deepEqual(
      out.map((r) => (r.job.data as { i: number }).i),
      items.map((x) => x.data.i),
    );
    await assert.rejects(
      () => q.addMany([...items, { name: "n", data: { i: -1 } }]),
      DurableQValidationError,
    );
    assert.equal(await q.count(), MAX_BATCH_SIZE);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("dedupe keys: duplicates within the batch and against existing jobs", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const existing = await q.add("n", { v: 0 }, { dedupeKey: "old" });
    const out = await q.addMany([
      { name: "n", data: { v: 1 }, options: { dedupeKey: "k" } },
      { name: "n", data: { v: 2 }, options: { dedupeKey: "k" } },
      { name: "n", data: { v: 3 }, options: { dedupeKey: "old" } },
      { name: "m", data: { v: 4 }, options: { dedupeKey: "k" } },
    ]);
    assert.deepEqual(
      out.map((r) => r.deduplicated),
      [false, true, true, false],
    );
    assert.equal(out[1]?.job.id, out[0]?.job.id);
    assert.deepEqual(out[1]?.job.data, { v: 1 }, "first writer wins");
    assert.equal(out[2]?.job.id, existing.job.id);
    assert.notEqual(out[3]?.job.id, out[0]?.job.id, "dedupe is scoped per job name");
    assert.equal(await q.count(), 3);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("invalid item rejects the whole batch before any write", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await assert.rejects(
      () =>
        q.addMany([
          { name: "n", data: { ok: 1 } },
          { name: "n", data: { bad: undefined } as unknown as null },
        ]),
      DurableQSerializationError,
    );
    await assert.rejects(
      () =>
        q.addMany([
          { name: "n", data: {} },
          { name: "", data: {} },
        ]),
      DurableQValidationError,
    );
    await assert.rejects(
      () => q.addMany([{ name: "n", data: {}, options: { priority: 1.5 } }]),
      DurableQValidationError,
    );
    await assert.rejects(
      () => q.addMany([null as unknown as { name: string; data: null }]),
      DurableQValidationError,
    );
    await assert.rejects(() => q.addMany("x" as unknown as []), DurableQValidationError);
    assert.equal(await q.count(), 0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("a failure inside the transaction rolls back every item", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { pre: true });
    await configureFault(q, { point: "enqueue:before-commit", action: "throw" });
    await assert.rejects(
      () => q.addMany(Array.from({ length: 50 }, (_, i) => ({ name: "n", data: { i } }))),
      /injected fault/,
    );
    await configureFault(q, { point: "enqueue:before-commit", action: "none" });
    assert.equal(await q.count(), 1);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

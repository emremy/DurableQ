import assert from "node:assert/strict";
import test from "node:test";
import { DurableQBusyError } from "../../src/errors.ts";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { configureFault } from "../support/test-control.ts";

const opts = { pollIntervalMs: 10, leaseDurationMs: 5_000, heartbeatIntervalMs: 100 };

test("transient SQLITE_BUSY on completion is retried; the handler runs once", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  let runs = 0;
  const errors: unknown[] = [];
  const worker = new Worker(q, { n: async () => (runs += 1) }, opts);
  worker.on("error", (e) => errors.push(e));
  try {
    const { job } = await q.add("n", {});
    await configureFault(q, { point: "settleSuccess:before-begin", action: "busy", times: 3 });
    await worker.start();
    await eventually(async () => assert.equal((await q.get(job.id))?.status, "completed"));
    assert.equal(runs, 1);
    assert.equal((await q.get(job.id))?.attemptCount, 1);
    assert.deepEqual(errors, []);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("persistent SQLITE_BUSY on completion surfaces an error and is not recorded as a failure", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  let runs = 0;
  const errors: unknown[] = [];
  const worker = new Worker(q, { n: async () => (runs += 1) }, opts);
  worker.on("error", (e) => errors.push(e));
  try {
    const { job } = await q.add("n", {});
    await configureFault(q, { point: "settleSuccess:before-begin", action: "busy" });
    await worker.start();
    await eventually(() => assert.equal(errors.length, 1), { timeoutMs: 10_000 });
    assert.ok(errors[0] instanceof DurableQBusyError);
    const got = await q.get(job.id);
    assert.equal(got?.status, "active", "left for lease recovery, not marked failed");
    assert.equal(got?.error, null);
    assert.equal(runs, 1);
  } finally {
    await configureFault(q, { point: "settleSuccess:before-begin", action: "none" });
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("transient SQLITE_BUSY on failure settlement is retried", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const errors: unknown[] = [];
  const worker = new Worker(
    q,
    {
      n: async () => {
        throw new Error("boom");
      },
    },
    opts,
  );
  worker.on("error", (e) => errors.push(e));
  try {
    const { job } = await q.add("n", {}, { attempts: 1 });
    await configureFault(q, { point: "settleFailure:before-begin", action: "busy", times: 2 });
    await worker.start();
    await eventually(async () => assert.equal((await q.get(job.id))?.status, "failed"));
    assert.equal((await q.get(job.id))?.error?.message, "boom");
    assert.deepEqual(errors, []);
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

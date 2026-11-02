import assert from "node:assert/strict";
import test from "node:test";
import { DurableQStaleLeaseError, DurableQValidationError } from "../../src/errors.ts";
import type { JobRow } from "../../src/storage/protocol.ts";
import type { JobContext } from "../../src/types.ts";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { rawRequest, tickLeases } from "../support/test-control.ts";

test("progress starts at 0, is visible while running and persists across restart", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const worker = new Worker(
    q,
    {
      n: async (_job, ctx) => {
        await ctx.updateProgress(10);
        await ctx.updateProgress(50);
        await gate;
        await ctx.updateProgress(100);
        return null;
      },
    },
    { pollIntervalMs: 10, leaseDurationMs: 5_000, heartbeatIntervalMs: 100 },
  );
  try {
    const { job } = await q.add("n", {});
    assert.equal(job.progress, 0);
    await worker.start();
    await eventually(async () => assert.equal((await q.get(job.id))?.progress, 50));
    release();
    await eventually(async () => assert.equal((await q.get(job.id))?.status, "completed"));
    await worker.close({ timeoutMs: 1_000 });
    await q.close();

    const reopened = openQueue(tmp.path);
    assert.equal((await reopened.get(job.id))?.progress, 100);
    await reopened.close();
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("invalid progress values are rejected without affecting the job", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const rejected: unknown[] = [];
  const worker = new Worker(
    q,
    {
      n: async (_job, ctx) => {
        for (const v of [-1, 101, 1.5, Number.NaN, "50" as unknown as number]) {
          await ctx.updateProgress(v).catch((e: unknown) => rejected.push(e));
        }
        await ctx.updateProgress(30);
        return null;
      },
    },
    { pollIntervalMs: 10, leaseDurationMs: 5_000, heartbeatIntervalMs: 100 },
  );
  try {
    const { job } = await q.add("n", {});
    await worker.start();
    await eventually(async () => assert.equal((await q.get(job.id))?.status, "completed"));
    assert.equal(rejected.length, 5);
    assert.ok(rejected.every((e) => e instanceof DurableQValidationError));
    assert.equal((await q.get(job.id))?.progress, 30);
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("retry resets progress at the next attempt; failure keeps the last value", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const seenAtStart: number[] = [];
  const worker = new Worker(
    q,
    {
      n: async (job, ctx) => {
        seenAtStart.push(job.progress);
        await ctx.updateProgress(ctx.attempt === 1 ? 40 : 70);
        throw new Error(`attempt ${ctx.attempt}`);
      },
    },
    { pollIntervalMs: 10, leaseDurationMs: 5_000, heartbeatIntervalMs: 100 },
  );
  try {
    const { job } = await q.add("n", {}, { attempts: 2, backoff: { type: "none" } });
    await worker.start();
    await eventually(async () => assert.equal((await q.get(job.id))?.status, "failed"));
    assert.deepEqual(seenAtStart, [0, 0]);
    assert.equal((await q.get(job.id))?.progress, 70);
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("updates after completion or cancellation are rejected as stale", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const contexts: JobContext[] = [];
  const worker = new Worker(
    q,
    {
      done: async (_job, ctx) => {
        contexts.push(ctx);
        await ctx.updateProgress(20);
        return null;
      },
      flaky: async (_job, ctx) => {
        contexts.push(ctx);
        await ctx.updateProgress(60);
        throw new Error("retry later");
      },
    },
    { pollIntervalMs: 10, leaseDurationMs: 5_000, heartbeatIntervalMs: 100 },
  );
  try {
    const done = await q.add("done", {});
    const flaky = await q.add(
      "flaky",
      {},
      { attempts: 3, backoff: { type: "fixed", delayMs: 60_000 } },
    );
    await worker.start();
    await eventually(async () => {
      assert.equal((await q.get(done.job.id))?.status, "completed");
      assert.equal((await q.get(flaky.job.id))?.attemptCount, 1);
      assert.equal((await q.get(flaky.job.id))?.status, "queued");
    });
    assert.deepEqual(await q.cancel(flaky.job.id), { cancelled: true });
    for (const ctx of contexts) {
      await assert.rejects(() => ctx.updateProgress(99), DurableQStaleLeaseError);
    }
    assert.equal((await q.get(done.job.id))?.progress, 20);
    assert.equal((await q.get(flaky.job.id))?.progress, 60);
    assert.equal((await q.get(flaky.job.id))?.status, "cancelled");
    assertDatabaseInvariants(tmp.path);
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("a stale lease generation cannot update progress", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", {}, { attempts: 3 });
    const t0 = Date.now();
    const claim = (workerId: string, nowMs: number) =>
      rawRequest<JobRow>(q, "claim", {
        nowMs,
        queue: "app",
        workerId,
        names: ["n"],
        leaseDurationMs: 100,
      });
    const progress = (row: JobRow, workerId: string, nowMs: number, value: number) =>
      rawRequest(q, "updateProgress", {
        nowMs,
        id: row.id,
        workerId,
        generation: row.lease_generation,
        progress: value,
      });
    const first = await claim("w1", t0);
    await progress(first, "w1", t0 + 10, 25);
    // Lease expires; another worker takes generation 2.
    const second = await claim("w2", t0 + 200);
    assert.equal(second.lease_generation, first.lease_generation + 1);
    await assert.rejects(() => progress(first, "w1", t0 + 210, 90), DurableQStaleLeaseError);
    await assert.rejects(() => progress(second, "w1", t0 + 210, 90), DurableQStaleLeaseError);
    await progress(second, "w2", t0 + 210, 5);
    assert.equal((await q.get(first.id))?.progress, 5);
    // Expired but not yet recovered: still rejected.
    await assert.rejects(() => progress(second, "w2", t0 + 400, 6), DurableQStaleLeaseError);
    await tickLeases(q);
    assertDatabaseInvariants(tmp.path, t0 + 400);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("a cached statement that failed (CHECK violation) is reusable afterwards", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", {});
    const now = Date.now();
    const row = await rawRequest<JobRow>(q, "claim", {
      nowMs: now,
      queue: "app",
      workerId: "w",
      names: ["n"],
      leaseDurationMs: 60_000,
    });
    const update = (progress: number) =>
      rawRequest(q, "updateProgress", {
        nowMs: now + 1,
        id: row.id,
        workerId: "w",
        generation: row.lease_generation,
        progress,
      });
    // Bypasses client-side validation to hit the schema CHECK inside the cached statement.
    await assert.rejects(() => update(500), /CHECK constraint failed/);
    await update(55);
    assert.equal((await q.get(row.id))?.progress, 55);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

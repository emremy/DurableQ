import assert from "node:assert/strict";
import test from "node:test";
import { createFrozenClock } from "../../src/clock.ts";
import type { JobRow } from "../../src/storage/protocol.ts";
import type { Queue } from "../../src/queue.ts";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { rawRequest } from "../support/test-control.ts";

function claim(q: Queue, nowMs = Date.now(), names = ["n"]): Promise<JobRow | null> {
  return rawRequest<JobRow | null>(q, "claim", {
    nowMs,
    queue: q.name,
    workerId: "w-test",
    names,
    leaseDurationMs: 60_000,
  });
}

test("pause blocks claims of pending jobs; enqueue still works; resume releases them", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { i: 1 });
    assert.equal(await q.isPaused(), false);
    await q.pause();
    assert.equal(await q.isPaused(), true);
    assert.equal(await claim(q), null);
    const added = await q.add("n", { i: 2 });
    assert.equal(added.job.status, "queued");
    assert.equal((await q.stats()).paused, true);
    assert.equal(await q.count({ status: "queued" }), 2);
    await q.resume();
    assert.ok(await claim(q));
    assert.ok(await claim(q));
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("repeated pause/resume calls are idempotent", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.resume();
    assert.equal(await q.isPaused(), false);
    await q.pause();
    await q.pause();
    assert.equal(await q.isPaused(), true);
    await q.resume();
    await q.resume();
    assert.equal(await q.isPaused(), false);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("pause state persists across restart and is per queue", async () => {
  const tmp = tempDbPath();
  try {
    const a = openQueue(tmp.path, "a");
    await a.pause();
    await a.add("n", {});
    await a.close();

    const a2 = openQueue(tmp.path, "a");
    const b = openQueue(tmp.path, "b");
    try {
      assert.equal(await a2.isPaused(), true);
      assert.equal(await b.isPaused(), false);
      await b.add("n", {});
      assert.ok(await claim(b));
      assert.equal(await claim(a2), null);
    } finally {
      await a2.close();
      await b.close();
    }
  } finally {
    tmp.cleanup();
  }
});

test("pause racing claims: once pause() resolves no further claim succeeds", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    for (let i = 0; i < 50; i++) await q.add("n", { i });
    const claims = Array.from({ length: 25 }, () => claim(q));
    await q.pause();
    await Promise.all(claims);
    const queuedAtPause = await q.count({ status: "queued" });
    const after = await Promise.all(Array.from({ length: 25 }, () => claim(q)));
    assert.ok(after.every((r) => r === null));
    assert.equal(await q.count({ status: "queued" }), queuedAtPause);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("active jobs keep running, heartbeating, reporting progress and settle while paused", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  let started = 0;
  const worker = new Worker(
    q,
    {
      n: async (_job, ctx) => {
        started += 1;
        await gate;
        await ctx.updateProgress(80);
        return { ok: true };
      },
    },
    { pollIntervalMs: 10, leaseDurationMs: 400, heartbeatIntervalMs: 50, concurrency: 2 },
  );
  try {
    const { job } = await q.add("n", { i: 1 });
    await worker.start();
    await eventually(() => assert.equal(started, 1));
    await q.pause();
    await q.add("n", { i: 2 });
    await new Promise((r) => setTimeout(r, 600)); // longer than the lease: heartbeats keep it
    assert.equal(started, 1, "no new claim while paused");
    release();
    await eventually(async () => assert.equal((await q.get(job.id))?.status, "completed"));
    assert.equal((await q.get(job.id))?.progress, 80);
    assert.equal(await q.count({ status: "queued" }), 1);
    await q.resume();
    await eventually(async () => assert.equal(await q.count({ status: "completed" }), 2));
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("resume after downtime: delayed jobs and schedules fire once, not per missed slot", async () => {
  const tmp = tempDbPath();
  const T0 = 1_800_000_000_000;
  const q = openQueue(tmp.path, "app", createFrozenClock(T0));
  try {
    await q.add("n", { delayed: true }, { delayMs: 1_000 });
    await q.schedule("tick", { id: "s", everyMs: 1_000, data: null });
    await q.pause();
    for (let t = 1; t <= 10; t++) assert.equal(await claim(q, T0 + t * 1_000, ["n", "tick"]), null);
    assert.equal(await q.count(), 1, "no occurrences generated while paused");
    await q.resume();
    const later = T0 + 10_500;
    const a = await claim(q, later, ["n", "tick"]);
    const b = await claim(q, later, ["n", "tick"]);
    assert.deepEqual([a?.name, b?.name].sort(), ["n", "tick"]);
    assert.equal(await claim(q, later, ["n", "tick"]), null);
    assert.equal((await q.getSchedule("s"))?.nextRunAt, T0 + 11_000);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

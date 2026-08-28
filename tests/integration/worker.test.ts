import assert from "node:assert/strict";
import test from "node:test";
import { NonRetryableError } from "../../src/errors.ts";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

test("worker success, retry, nonretryable, maxAttempts", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const seen: string[] = [];
  const worker = new Worker(
    q,
    {
      ok: async () => ({ done: true }),
      flaky: async (job) => {
        seen.push(job.id);
        if (seen.filter((id) => id === job.id).length < 2) throw new Error("once");
        return { ok: true };
      },
      dead: async () => {
        throw new NonRetryableError("nope");
      },
      always: async () => {
        throw new Error("fail");
      },
    },
    { concurrency: 2, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    await q.add("ok", { a: 1 });
    await q.add("flaky", { a: 1 }, { attempts: 5, backoff: { type: "none" } });
    await q.add("dead", { a: 1 }, { attempts: 5 });
    const always = await q.add("always", { a: 1 }, { attempts: 2, backoff: { type: "none" } });
    const errors: unknown[] = [];
    worker.on("error", (e) => errors.push(e));
    await worker.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "completed" }), 2, `errors=${errors.join(",")}`);
      assert.equal(await q.count({ status: "failed" }), 2);
    });
    const failed = await q.get(always.job.id);
    assert.equal(failed?.status, "failed");
    assert.equal(failed?.attemptCount, 2);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("INV-018/019 shutdown stops claims and finishes short jobs", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  let started = 0;
  const worker = new Worker(
    q,
    {
      slow: async (_job, ctx) => {
        started += 1;
        await new Promise((r) => setTimeout(r, 80));
        if (ctx.signal.aborted) throw new Error("aborted");
        return { ok: true };
      },
    },
    { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    await q.add("slow", { n: 1 });
    await q.add("slow", { n: 2 });
    await worker.start();
    await eventually(() => {
      assert.equal(started, 1);
    });
    await worker.close({ timeoutMs: 2_000 });
    assert.equal(started, 1);
    const queued = await q.count({ status: "queued" });
    const completed = await q.count({ status: "completed" });
    assert.equal(queued + completed, 2);
    assert.equal(completed, 1);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("shutdown timeout aborts a stuck handler", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    {
      stuck: async (_job, ctx) => {
        await new Promise((r) => {
          const t = setTimeout(r, 10_000);
          ctx.signal.addEventListener("abort", () => {
            clearTimeout(t);
            r(undefined);
          });
        });
        return { ok: false };
      },
    },
    { concurrency: 1, pollIntervalMs: 15, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  worker.on("completed", () => {
    throw new Error("listener boom");
  });
  worker.on("error", () => {
    /* swallow listener failure */
  });
  try {
    await q.add("stuck", { n: 1 });
    await worker.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "active" }), 1);
    });
    await worker.close({ timeoutMs: 40 });
    assert.equal(await q.count({ status: "completed" }), 0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("non-serializable result is terminal", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    {
      bad: async () => new Date() as unknown as { x: number },
    },
    { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    const added = await q.add("bad", { a: 1 }, { attempts: 5 });
    await worker.start();
    await eventually(async () => {
      const job = await q.get(added.job.id);
      assert.equal(job?.status, "failed");
      assert.equal(job?.attemptCount, 1);
    });
    assertDatabaseInvariants(tmp.path);
  } finally {
    await worker.close({ timeoutMs: 500 });
    await q.close();
    tmp.cleanup();
  }
});

test("shutdown with zero jobs", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    { n: async () => ({ ok: true }) },
    { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    await worker.start();
    await worker.close({ timeoutMs: 200 });
    assert.equal(await q.count(), 0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("handler that ignores abort does not hang close", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    {
      stuck: async () => {
        await new Promise((r) => setTimeout(r, 400));
        return { ok: true };
      },
    },
    { concurrency: 1, pollIntervalMs: 15, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    await q.add("stuck", { n: 1 });
    await worker.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "active" }), 1);
    });
    const start = Date.now();
    await worker.close({ timeoutMs: 30 });
    assert.ok(Date.now() - start < 800);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("attempt timeout aborts handler without fake-complete", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    {
      slow: async (_job, ctx) => {
        await new Promise((r) => {
          const t = setTimeout(r, 10_000);
          ctx.signal.addEventListener("abort", () => {
            clearTimeout(t);
            r(undefined);
          });
        });
        return { ok: true };
      },
    },
    {
      concurrency: 1,
      pollIntervalMs: 15,
      leaseDurationMs: 5_000,
      heartbeatIntervalMs: 50,
      defaultAttemptTimeoutMs: 80,
    },
  );
  try {
    await q.add("slow", { n: 1 }, { attemptTimeoutMs: 80, attempts: 3 });
    await worker.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "active" }), 1);
    });
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(await q.count({ status: "completed" }), 0);
  } finally {
    await worker.close({ timeoutMs: 200 });
    await q.close();
    tmp.cleanup();
  }
});

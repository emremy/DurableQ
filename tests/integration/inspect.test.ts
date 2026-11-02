import assert from "node:assert/strict";
import test from "node:test";
import { createFrozenClock } from "../../src/clock.ts";
import { DurableQValidationError } from "../../src/errors.ts";
import type { JobRow } from "../../src/storage/protocol.ts";
import type { Queue } from "../../src/queue.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { rawRequest } from "../support/test-control.ts";

const T0 = 1_800_000_000_000;

async function claimAndSettle(
  q: Queue,
  name: string,
  nowMs: number,
  outcome: "ok" | "fail" | "retry" | "hold",
): Promise<JobRow> {
  const row = await rawRequest<JobRow>(q, "claim", {
    nowMs,
    queue: q.name,
    workerId: "w",
    names: [name],
    leaseDurationMs: 60 * 60 * 1000,
  });
  const base = { nowMs: nowMs + 1, id: row.id, workerId: "w", generation: row.lease_generation };
  if (outcome === "ok") await rawRequest(q, "settleSuccess", { ...base, resultJson: "1" });
  if (outcome === "fail" || outcome === "retry") {
    await rawRequest(q, "settleFailure", {
      ...base,
      error: { name: "Error", message: outcome },
      nonRetryable: outcome === "fail",
    });
  }
  return row;
}

test("stats on an empty database", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    assert.deepEqual(await q.stats(), {
      queued: 0,
      active: 0,
      completed: 0,
      failed: 0,
      cancelled: 0,
      due: 0,
      delayed: 0,
      retrying: 0,
      oldestQueuedAgeMs: null,
      paused: false,
    });
    assert.deepEqual(await q.list(), { jobs: [], nextCursor: null });
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("stats reflect every status, delayed/due/retrying subsets and queue isolation", async () => {
  const tmp = tempDbPath();
  const clock = createFrozenClock(T0);
  const q = openQueue(tmp.path, "app", clock);
  const other = openQueue(tmp.path, "other", clock);
  try {
    await other.add("x", {});
    await q.add("due", {});
    clock.advance(5_000);
    await q.add("due", {});
    await q.add("later", {}, { delayMs: 60_000 });
    await q.add("ok", {});
    await q.add("fail", {});
    await q.add("retry", {}, { backoff: { type: "fixed", delayMs: 10 } });
    await q.add("hold", {});
    const cancelled = await q.add("cancel", {});
    await q.cancel(cancelled.job.id);
    await claimAndSettle(q, "ok", T0 + 5_000, "ok");
    await claimAndSettle(q, "fail", T0 + 5_000, "fail");
    await claimAndSettle(q, "retry", T0 + 5_000, "retry");
    await claimAndSettle(q, "hold", T0 + 5_000, "hold");
    clock.set(T0 + 7_000);
    const s = await q.stats();
    assert.deepEqual(s, {
      queued: 4,
      active: 1,
      completed: 1,
      failed: 1,
      cancelled: 1,
      due: 3,
      delayed: 1,
      retrying: 1,
      oldestQueuedAgeMs: 7_000,
      paused: false,
    });
    assert.equal(s.due + s.delayed, s.queued);
    assert.equal((await other.stats()).queued, 1);
  } finally {
    await q.close();
    await other.close();
    tmp.cleanup();
  }
});

test("list filters by status and name; failed shows terminal jobs only", async () => {
  const tmp = tempDbPath();
  const clock = createFrozenClock(T0);
  const q = openQueue(tmp.path, "app", clock);
  try {
    await q.add("fail", { i: 1 });
    await q.add("retry", { i: 2 }, { backoff: { type: "fixed", delayMs: 10 } });
    await q.add("email.send", { i: 3 });
    await claimAndSettle(q, "fail", T0, "fail");
    await claimAndSettle(q, "retry", T0, "retry");
    const failed = await q.list({ status: "failed" });
    assert.deepEqual(
      failed.jobs.map((j) => [j.name, j.error?.message]),
      [["fail", "fail"]],
    );
    const byName = await q.list({ name: "email.send" });
    assert.deepEqual(
      byName.jobs.map((j) => j.data),
      [{ i: 3 }],
    );
    const both = await q.list({ name: "retry", status: "queued" });
    assert.equal(both.jobs[0]?.attemptCount, 1);
    assert.equal((await q.list({ name: "retry", status: "failed" })).jobs.length, 0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("pagination is complete, ordered, and stable under concurrent inserts", async () => {
  const tmp = tempDbPath();
  const clock = createFrozenClock(T0);
  const q = openQueue(tmp.path, "app", clock);
  try {
    for (let i = 0; i < 95; i++) {
      if (i % 10 === 0) clock.advance(1); // many ties on updated_at
      await q.add(i % 2 ? "a" : "b", { i });
    }
    for (let i = 0; i < 10; i++) await claimAndSettle(q, "a", T0 + 100 + i, "ok");

    for (const filter of [{}, { name: "a" }, { status: "queued" as const }]) {
      const seen: number[] = [];
      let cursor: string | null = null;
      let pages = 0;
      let lastKey: [number, number] | null = null;
      do {
        const page: Awaited<ReturnType<typeof q.list>> = await q.list({
          ...filter,
          limit: 7,
          ...(cursor ? { cursor } : {}),
        });
        for (const j of page.jobs) {
          seen.push((j.data as { i: number }).i);
          if (lastKey) assert.ok(j.updatedAt <= lastKey[0], "descending updatedAt");
          lastKey = [j.updatedAt, 0];
        }
        // Concurrent writes between pages must not duplicate or skip pre-existing jobs.
        await q.add("a", { i: 1_000 + pages });
        cursor = page.nextCursor;
        pages += 1;
      } while (cursor);
      const unique = new Set(seen.filter((i) => i < 1_000));
      assert.equal(unique.size, seen.filter((i) => i < 1_000).length, "no duplicates");
      const total =
        "name" in filter
          ? 47 // odd i in 0..94
          : "status" in filter
            ? 85
            : 95;
      assert.equal(unique.size, total, JSON.stringify(filter));
    }
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("invalid list options and limits are rejected", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    for (const opts of [
      { status: "running" },
      { limit: 0 },
      { limit: 1_001 },
      { limit: 1.5 },
      { name: "" },
      { cursor: "abc" },
      { cursor: "1.2.3" },
      { cursor: 5 },
    ]) {
      await assert.rejects(
        () => q.list(opts as Parameters<typeof q.list>[0]),
        DurableQValidationError,
        JSON.stringify(opts),
      );
    }
    assert.equal((await q.list({ limit: 1_000 })).jobs.length, 0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("pagination across statuses with identical updatedAt never skips or repeats", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path, "app", createFrozenClock(T0));
  try {
    const ids: string[] = [];
    for (let i = 0; i < 12; i++) ids.push((await q.add("n", { i })).job.id);
    // Same instant: cancelled and queued rows share updatedAt, so ordering falls to the tie-break.
    for (const id of ids.filter((_, i) => i % 3 === 0)) await q.cancel(id);
    for (const limit of [1, 2, 3, 5, 12]) {
      const seen: string[] = [];
      let cursor: string | undefined;
      let pages = 0;
      for (;;) {
        const page = await q.list({ limit, ...(cursor ? { cursor } : {}) });
        pages += 1;
        seen.push(...page.jobs.map((j) => j.id));
        if (!page.nextCursor) break;
        cursor = page.nextCursor;
      }
      assert.deepEqual([...seen].sort(), [...ids].sort(), `limit ${limit}`);
      assert.equal(
        pages,
        Math.ceil(ids.length / limit),
        `no trailing empty page at limit ${limit}`,
      );
    }
    assert.equal((await q.list({ limit: 12 })).nextCursor, null);
    assert.notEqual((await q.list({ limit: 11 })).nextCursor, null);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

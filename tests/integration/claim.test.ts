import assert from "node:assert/strict";
import test from "node:test";
import { DurableQStaleLeaseError } from "../../src/errors.ts";
import { createFrozenClock } from "../../src/clock.ts";
import { Queue } from "../../src/queue.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { configureFault, rawRequest, setStorageNow, tickLeases } from "../support/test-control.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

async function claim(
  q: ReturnType<typeof openQueue>,
  names = ["n"],
  lease = 30_000,
  now = Date.now(),
) {
  return rawRequest(q, "claim", {
    nowMs: now,
    queue: q.name,
    workerId: "w1",
    names,
    leaseDurationMs: lease,
  });
}

test("INV-006/008/013/024 claim increments generation and filters names", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const low = await q.add("n", { i: 1 }, { priority: 1 });
    const high = await q.add("n", { i: 2 }, { priority: 10 });
    await q.add("other", { i: 3 }, { priority: 100 });
    const c1 = await claim(q, ["n"]);
    assert.ok(c1);
    const row = c1 as {
      id: string;
      attempt_count: number;
      lease_generation: number;
      priority: number;
    };
    assert.equal(row.id, high.job.id);
    assert.equal(row.attempt_count, 1);
    assert.equal(row.lease_generation, 1);
    const c2 = await claim(q, ["n"]);
    assert.equal((c2 as { id: string }).id, low.job.id);
    const c3 = await claim(q, ["n"]);
    assert.equal(c3, null);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("INV-026 heartbeat and settle reject expired lease before generation bump", async () => {
  const tmp = tempDbPath();
  const t0 = 1_000_000;
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 }, createFrozenClock(t0));
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  try {
    await setStorageNow(q, t0);
    await q.add("n", { i: 1 });
    const claimed = (await claim(q, ["n"], 100, t0)) as {
      id: string;
      lease_generation: number;
      lease_expires_at: number;
    };
    assert.ok(claimed);
    assert.equal(claimed.lease_generation, 1);
    const hbOk = await rawRequest(q, "heartbeat", {
      nowMs: claimed.lease_expires_at,
      id: claimed.id,
      workerId: "w1",
      generation: 1,
      leaseDurationMs: 100,
    });
    assert.equal(hbOk, false);
    await assert.rejects(
      () =>
        rawRequest(q, "settleSuccess", {
          nowMs: claimed.lease_expires_at,
          id: claimed.id,
          workerId: "w1",
          generation: 1,
          resultJson: "{}",
        }),
      DurableQStaleLeaseError,
    );
    await setStorageNow(q, claimed.lease_expires_at);
    await tickLeases(q);
    const after = await q.get(claimed.id);
    assert.ok(after);
    assert.notEqual(after.status, "completed");
    assertDatabaseInvariants(tmp.path, claimed.lease_expires_at);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("INV-004 stale completion after newer generation", async () => {
  const tmp = tempDbPath();
  const t0 = 2_000_000;
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 }, createFrozenClock(t0));
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  try {
    await setStorageNow(q, t0);
    await q.add("n", { i: 1 }, { attempts: 3 });
    const a = (await claim(q, ["n"], 50, t0)) as { id: string; lease_generation: number };
    assert.ok(a);
    await setStorageNow(q, t0 + 50);
    await tickLeases(q);
    const b = (await rawRequest(q, "claim", {
      nowMs: t0 + 50,
      queue: q.name,
      workerId: "w2",
      names: ["n"],
      leaseDurationMs: 5_000,
    })) as { lease_generation: number; id: string };
    assert.equal(b.lease_generation, 2);
    await assert.rejects(
      () =>
        rawRequest(q, "settleSuccess", {
          nowMs: t0 + 50,
          id: a.id,
          workerId: "w1",
          generation: 1,
          resultJson: "{}",
        }),
      DurableQStaleLeaseError,
    );
    const done = await rawRequest(q, "settleSuccess", {
      nowMs: t0 + 50,
      id: b.id,
      workerId: "w2",
      generation: 2,
      resultJson: '{"ok":true}',
    });
    assert.equal((done as { status: string }).status, "completed");
    assertDatabaseInvariants(tmp.path, t0 + 50);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("D-006 claim F1-F3 no claim; F4 may claim", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { i: 1 });
    for (const point of [
      "claim:before-begin",
      "claim:after-begin",
      "claim:before-commit",
    ] as const) {
      await configureFault(q, { point, action: "throw" });
      await assert.rejects(() => claim(q));
      await configureFault(q, { point, action: "none" });
    }
    const still = await q.get((await q.add("n", { i: 2 })).job.id);
    assert.ok(still);
    await configureFault(q, { point: "claim:after-commit", action: "throw" });
    await q.add("only", { i: 3 });
    await assert.rejects(() =>
      rawRequest(q, "claim", {
        nowMs: Date.now(),
        queue: q.name,
        workerId: "w1",
        names: ["only"],
        leaseDurationMs: 30_000,
      }),
    );
    await configureFault(q, { point: "claim:after-commit", action: "none" });
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

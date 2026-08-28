import assert from "node:assert/strict";
import test from "node:test";
import { Queue } from "../../src/queue.ts";
import { createFrozenClock } from "../../src/clock.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { setStorageNow, tickLeases, rawRequest } from "../support/test-control.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { DurableQStaleLeaseError } from "../../src/errors.ts";

test("expired last attempt becomes failed", async () => {
  const tmp = tempDbPath();
  const t0 = 5_000_000;
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 }, createFrozenClock(t0));
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  try {
    await setStorageNow(q, t0);
    const { job } = await q.add("n", { v: 1 }, { attempts: 1 });
    await rawRequest(q, "claim", {
      nowMs: t0,
      queue: q.name,
      workerId: "w",
      names: ["n"],
      leaseDurationMs: 10,
    });
    await setStorageNow(q, t0 + 10);
    await tickLeases(q);
    const got = await q.get(job.id);
    assert.equal(got?.status, "failed");
    assertDatabaseInvariants(tmp.path, t0 + 10);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("idempotent close", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  await q.count();
  await q.close();
  await q.close();
  tmp.cleanup();
});

test("old handler overlap cannot ACK after recovery", async () => {
  const tmp = tempDbPath();
  const t0 = 6_000_000;
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 }, createFrozenClock(t0));
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  try {
    await setStorageNow(q, t0);
    const { job } = await q.add("n", { v: 1 }, { attempts: 3 });
    await rawRequest(q, "claim", {
      nowMs: t0,
      queue: q.name,
      workerId: "old",
      names: ["n"],
      leaseDurationMs: 20,
    });
    await setStorageNow(q, t0 + 20);
    await tickLeases(q);
    const queued = await q.get(job.id);
    assert.equal(queued?.status, "queued");
    await assert.rejects(
      () =>
        rawRequest(q, "settleSuccess", {
          nowMs: t0 + 20,
          id: job.id,
          workerId: "old",
          generation: 1,
          resultJson: "{}",
        }),
      DurableQStaleLeaseError,
    );
    const next = (await rawRequest(q, "claim", {
      nowMs: t0 + 20,
      queue: q.name,
      workerId: "new",
      names: ["n"],
      leaseDurationMs: 5_000,
    })) as { lease_generation: number };
    assert.equal(next.lease_generation, 2);
    assertDatabaseInvariants(tmp.path, t0 + 20);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

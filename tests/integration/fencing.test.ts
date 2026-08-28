import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { configureFault, rawRequest } from "../support/test-control.ts";
import { DurableQStaleLeaseError } from "../../src/errors.ts";

test("heartbeat starvation under write lock does not resurrect expired lease", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { v: 1 }, { attempts: 3 });
    const nowClaim = Date.now();
    const claimed = (await rawRequest(q, "claim", {
      nowMs: nowClaim,
      queue: q.name,
      workerId: "w1",
      names: ["n"],
      leaseDurationMs: 80,
    })) as { id: string; lease_generation: number; lease_expires_at: number };

    const locker = new DatabaseSync(tmp.path, { timeout: 1 });
    locker.exec("BEGIN IMMEDIATE");
    await new Promise((r) => setTimeout(r, 120));
    locker.exec("COMMIT");
    locker.close();

    const now = Date.now();
    const hb = await rawRequest(q, "heartbeat", {
      nowMs: now,
      id: claimed.id,
      workerId: "w1",
      generation: claimed.lease_generation,
      leaseDurationMs: 80,
    });
    assert.equal(hb, false);
    await assert.rejects(
      () =>
        rawRequest(q, "settleSuccess", {
          nowMs: now,
          id: claimed.id,
          workerId: "w1",
          generation: claimed.lease_generation,
          resultJson: "{}",
        }),
      DurableQStaleLeaseError,
    );
    await rawRequest(q, "recoverExpired", { nowMs: now });
    const job = await q.get(claimed.id);
    assert.ok(job);
    assert.notEqual(job.status, "completed");
    assert.notEqual(job.status, "active");
    assertDatabaseInvariants(tmp.path, now);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("settlement F1-F3 do not complete", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { v: 1 });
    const claimed = (await rawRequest(q, "claim", {
      nowMs: Date.now(),
      queue: q.name,
      workerId: "w1",
      names: ["n"],
      leaseDurationMs: 30_000,
    })) as { id: string; lease_generation: number };
    await configureFault(q, { point: "settleSuccess:before-commit", action: "throw" });
    await assert.rejects(() =>
      rawRequest(q, "settleSuccess", {
        nowMs: Date.now(),
        id: claimed.id,
        workerId: "w1",
        generation: claimed.lease_generation,
        resultJson: "{}",
      }),
    );
    await configureFault(q, { point: "settleSuccess:before-commit", action: "none" });
    const job = await q.get(claimed.id);
    assert.equal(job?.status, "active");
    await rawRequest(q, "settleSuccess", {
      nowMs: Date.now(),
      id: claimed.id,
      workerId: "w1",
      generation: claimed.lease_generation,
      resultJson: "{}",
    });
    assert.equal((await q.get(claimed.id))?.status, "completed");
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("settlement F4 commit is durable while caller sees failure", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { v: 1 });
    const claimed = (await rawRequest(q, "claim", {
      nowMs: Date.now(),
      queue: q.name,
      workerId: "w1",
      names: ["n"],
      leaseDurationMs: 30_000,
    })) as { id: string; lease_generation: number };
    await configureFault(q, { point: "settleSuccess:after-commit", action: "throw" });
    await assert.rejects(() =>
      rawRequest(q, "settleSuccess", {
        nowMs: Date.now(),
        id: claimed.id,
        workerId: "w1",
        generation: claimed.lease_generation,
        resultJson: "{}",
      }),
    );
    await configureFault(q, { point: "settleSuccess:after-commit", action: "none" });
    assert.equal((await q.get(claimed.id))?.status, "completed");
    await assert.rejects(
      () =>
        rawRequest(q, "settleSuccess", {
          nowMs: Date.now(),
          id: claimed.id,
          workerId: "w1",
          generation: claimed.lease_generation,
          resultJson: "{}",
        }),
      DurableQStaleLeaseError,
    );
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { createFrozenClock } from "../../src/clock.ts";
import { DurableQStaleLeaseError } from "../../src/errors.ts";
import { Queue } from "../../src/queue.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { rawRequest, setStorageNow, tickLeases } from "../support/test-control.ts";
import { tempDbPath } from "../support/temp-db.ts";

const T0 = 4_000_000;
const LEASE = 30_000;

type Claimed = {
  id: string;
  attempt_count: number;
  lease_generation: number;
  lease_owner: string;
  lease_expires_at: number;
  status: string;
};

async function openFrozen(path: string): Promise<Queue> {
  const q = new Queue("app", { path, busyTimeoutMs: 50 }, createFrozenClock(T0));
  await setStorageNow(q, T0);
  return q;
}

async function claim(
  q: Queue,
  names: string[],
  nowMs = T0,
  workerId = "w1",
  leaseDurationMs = LEASE,
): Promise<Claimed | null> {
  return rawRequest<Claimed | null>(q, "claim", {
    nowMs,
    queue: q.name,
    workerId,
    names,
    leaseDurationMs,
  });
}

function attemptRow(
  dbPath: string,
  jobId: string,
): { finished_at: number | null; outcome: string | null; error_json: string | null } | undefined {
  const db = new DatabaseSync(dbPath, { timeout: 5000, defensive: true, allowExtension: false });
  try {
    return db
      .prepare(
        "SELECT finished_at, outcome, error_json FROM durableq_attempts WHERE job_id = ? ORDER BY lease_generation DESC LIMIT 1",
      )
      .get(jobId) as
      { finished_at: number | null; outcome: string | null; error_json: string | null } | undefined;
  } finally {
    db.close();
  }
}

test("claim empty names, delay arithmetic, and wake hint", async () => {
  const tmp = tempDbPath();
  const q = await openFrozen(tmp.path);
  try {
    const empty = await claim(q, []);
    assert.equal(empty, null);

    const delayed = await q.add("n", { v: 1 }, { delayMs: 5_000 });
    assert.equal(delayed.job.availableAt, T0 + 5_000);
    assert.equal(await claim(q, ["n"], T0), null);

    const futureHint = await rawRequest<number | null>(q, "nextWakeHint", {
      queue: q.name,
      names: ["n"],
      nowMs: T0,
    });
    assert.equal(futureHint, T0 + 5_000);

    const emptyHint = await rawRequest<number | null>(q, "nextWakeHint", {
      queue: q.name,
      names: [],
      nowMs: T0,
    });
    assert.equal(emptyHint, null);

    const claimed = await claim(q, ["n"], T0 + 5_000);
    assert.ok(claimed);
    assert.equal(claimed.id, delayed.job.id);
    assert.equal(claimed.status, "active");
    assert.equal(claimed.attempt_count, 1);
    assert.equal(claimed.lease_generation, 1);
    assert.equal(claimed.lease_owner, "w1");
    assert.equal(claimed.lease_expires_at, T0 + 5_000 + LEASE);

    const ready = await q.add("n", { v: 2 });
    const pastDueHint = await rawRequest<number | null>(q, "nextWakeHint", {
      queue: q.name,
      names: ["n"],
      nowMs: T0 + 100,
    });
    assert.equal(pastDueHint, T0 + 100);
    assert.equal(ready.job.availableAt, T0);
    assertDatabaseInvariants(tmp.path, T0 + 5_000);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("cancel and retry reasons plus max_attempts bump", async () => {
  const tmp = tempDbPath();
  const q = await openFrozen(tmp.path);
  try {
    const missingCancel = await q.cancel("no-such-job");
    assert.equal(missingCancel.cancelled, false);
    if (!missingCancel.cancelled) assert.equal(missingCancel.reason, "missing");

    const queued = await q.add("n", { v: 1 }, { attempts: 3 });
    const queuedRetry = await q.retry(queued.job.id);
    assert.equal(queuedRetry.retried, false);
    if (!queuedRetry.retried) assert.equal(queuedRetry.reason, "queued");

    const missingRetry = await q.retry("no-such-job");
    assert.equal(missingRetry.retried, false);
    if (!missingRetry.retried) assert.equal(missingRetry.reason, "missing");

    const cancelled = await q.cancel(queued.job.id);
    assert.equal(cancelled.cancelled, true);
    const again = await q.cancel(queued.job.id);
    assert.equal(again.cancelled, false);
    if (!again.cancelled) assert.equal(again.reason, "cancelled");
    const retryCancelled = await q.retry(queued.job.id);
    assert.equal(retryCancelled.retried, false);
    if (!retryCancelled.retried) assert.equal(retryCancelled.reason, "cancelled");

    const activeJob = await q.add("n", { v: 2 }, { attempts: 3 });
    const activeClaim = await claim(q, ["n"]);
    assert.ok(activeClaim);
    assert.equal(activeClaim.id, activeJob.job.id);
    const cancelActive = await q.cancel(activeJob.job.id);
    assert.equal(cancelActive.cancelled, false);
    if (!cancelActive.cancelled) assert.equal(cancelActive.reason, "active");
    const retryActive = await q.retry(activeJob.job.id);
    assert.equal(retryActive.retried, false);
    if (!retryActive.retried) assert.equal(retryActive.reason, "active");

    await rawRequest(q, "settleSuccess", {
      nowMs: T0,
      id: activeClaim.id,
      workerId: "w1",
      generation: activeClaim.lease_generation,
      resultJson: "{}",
    });
    const cancelCompleted = await q.cancel(activeJob.job.id);
    assert.equal(cancelCompleted.cancelled, false);
    if (!cancelCompleted.cancelled) assert.equal(cancelCompleted.reason, "completed");
    const retryCompleted = await q.retry(activeJob.job.id);
    assert.equal(retryCompleted.retried, false);
    if (!retryCompleted.retried) assert.equal(retryCompleted.reason, "completed");

    const failJob = await q.add("fail", { v: 3 }, { attempts: 3 });
    const failClaim = await claim(q, ["fail"]);
    assert.ok(failClaim);
    assert.equal(failClaim.id, failJob.job.id);
    await rawRequest(q, "settleFailure", {
      nowMs: T0,
      id: failClaim.id,
      workerId: "w1",
      generation: failClaim.lease_generation,
      error: { name: "Error", message: "boom" },
      nonRetryable: true,
    });
    const cancelFailed = await q.cancel(failJob.job.id);
    assert.equal(cancelFailed.cancelled, false);
    if (!cancelFailed.cancelled) assert.equal(cancelFailed.reason, "failed");
    const retried = await q.retry(failJob.job.id);
    assert.equal(retried.retried, true);
    if (retried.retried) {
      assert.equal(retried.job.status, "queued");
      assert.equal(retried.job.error, null);
      assert.equal(retried.job.maxAttempts, 3);
      assert.equal(retried.job.attemptCount, 1);
    }

    const exhausted = await q.add("exh", { v: 4 }, { attempts: 1 });
    const exhaustedClaim = await claim(q, ["exh"]);
    assert.ok(exhaustedClaim);
    assert.equal(exhaustedClaim.id, exhausted.job.id);
    await rawRequest(q, "settleFailure", {
      nowMs: T0,
      id: exhaustedClaim.id,
      workerId: "w1",
      generation: exhaustedClaim.lease_generation,
      error: { name: "Error", message: "done" },
      nonRetryable: false,
    });
    const bumped = await q.retry(exhausted.job.id);
    assert.equal(bumped.retried, true);
    if (bumped.retried) {
      assert.equal(bumped.job.maxAttempts, 2);
      assert.equal(bumped.job.attemptCount, 1);
      assert.equal(bumped.job.error, null);
    }
    assertDatabaseInvariants(tmp.path, T0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("clean deletes only the requested terminal status", async () => {
  const tmp = tempDbPath();
  const q = await openFrozen(tmp.path);
  try {
    const a = await q.add("n", { v: 1 });
    const b = await q.add("n", { v: 2 });
    const c = await q.add("n", { v: 3 });
    const first = await claim(q, ["n"]);
    assert.ok(first);
    await rawRequest(q, "settleSuccess", {
      nowMs: T0,
      id: first.id,
      workerId: "w1",
      generation: first.lease_generation,
      resultJson: "{}",
    });
    const second = await claim(q, ["n"]);
    assert.ok(second);
    await rawRequest(q, "settleFailure", {
      nowMs: T0,
      id: second.id,
      workerId: "w1",
      generation: second.lease_generation,
      error: { name: "Error", message: "x" },
      nonRetryable: true,
    });
    const leftoverId = [a.job.id, b.job.id, c.job.id].find(
      (id) => id !== first.id && id !== second.id,
    );
    assert.ok(leftoverId);
    const cancelled = await q.cancel(leftoverId);
    assert.equal(cancelled.cancelled, true);

    const delCompleted = await q.clean({ status: "completed", before: T0 + 1 });
    assert.equal(delCompleted.deleted, 1);
    assert.equal(await q.get(first.id), null);
    assert.equal((await q.get(second.id))?.status, "failed");
    assert.equal((await q.get(leftoverId))?.status, "cancelled");

    const delFailed = await q.clean({ status: "failed", before: T0 + 1 });
    assert.equal(delFailed.deleted, 1);
    assert.equal(await q.get(second.id), null);
    assert.equal((await q.get(leftoverId))?.status, "cancelled");

    const delCancelled = await q.clean({ status: "cancelled", before: T0 + 1 });
    assert.equal(delCancelled.deleted, 1);
    assert.equal(await q.get(leftoverId), null);
    assertDatabaseInvariants(tmp.path, T0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("settleFailure retry delay, fencing messages, and recovery attempt rows", async () => {
  const tmp = tempDbPath();
  const q = await openFrozen(tmp.path);
  try {
    const retryJob = await q.add(
      "n",
      { v: 1 },
      {
        attempts: 3,
        backoff: { type: "fixed", delayMs: 2_000 },
      },
    );
    const claimed = await claim(q, ["n"], T0, "w1", 500);
    assert.ok(claimed);
    assert.equal(claimed.id, retryJob.job.id);
    const hb = await rawRequest<boolean>(q, "heartbeat", {
      nowMs: T0,
      id: claimed.id,
      workerId: "w1",
      generation: claimed.lease_generation,
      leaseDurationMs: 500,
    });
    assert.equal(hb, true);
    const afterHb = await q.get(retryJob.job.id);
    assert.equal(afterHb?.leaseExpiresAt, T0 + 500);

    await rawRequest(q, "settleFailure", {
      nowMs: T0,
      id: claimed.id,
      workerId: "w1",
      generation: claimed.lease_generation,
      error: { name: "Error", message: "retry-me" },
      nonRetryable: false,
    });
    const requeued = await q.get(retryJob.job.id);
    assert.equal(requeued?.status, "queued");
    assert.equal(requeued?.availableAt, T0 + 2_000);
    assert.equal(requeued?.leaseOwner, null);
    const retryAttempt = attemptRow(tmp.path, retryJob.job.id);
    assert.equal(retryAttempt?.outcome, "failed_retrying");
    assert.equal(retryAttempt?.finished_at, T0);

    await assert.rejects(
      () =>
        rawRequest(q, "settleSuccess", {
          nowMs: T0,
          id: claimed.id,
          workerId: "w1",
          generation: claimed.lease_generation,
          resultJson: "{}",
        }),
      (err: unknown) => {
        assert.equal(err instanceof DurableQStaleLeaseError, true);
        assert.match(String(err), /completion rejected/);
        return true;
      },
    );
    await assert.rejects(
      () =>
        rawRequest(q, "settleFailure", {
          nowMs: T0,
          id: claimed.id,
          workerId: "w1",
          generation: claimed.lease_generation,
          error: { name: "Error", message: "stale" },
          nonRetryable: false,
        }),
      (err: unknown) => {
        assert.equal(err instanceof DurableQStaleLeaseError, true);
        assert.match(String(err), /failure settlement rejected/);
        return true;
      },
    );

    const recoverJob = await q.add(
      "n",
      { v: 2 },
      {
        attempts: 3,
        backoff: { type: "fixed", delayMs: 3_000 },
      },
    );
    const recoverClaim = await claim(q, ["n"], T0, "w2", 10);
    assert.ok(recoverClaim);
    assert.equal(recoverClaim.id, recoverJob.job.id);
    await setStorageNow(q, T0 + 10);
    const recovered = await rawRequest<number>(q, "recoverExpired", { nowMs: T0 + 10 });
    assert.equal(recovered, 1);
    const afterRecover = await q.get(recoverJob.job.id);
    assert.equal(afterRecover?.status, "queued");
    assert.equal(afterRecover?.availableAt, T0 + 10 + 3_000);
    assert.equal(afterRecover?.error?.name, "LeaseExpiredError");
    assert.equal(afterRecover?.error?.message, "lease expired");
    const recoveredAttempt = attemptRow(tmp.path, recoverJob.job.id);
    assert.equal(recoveredAttempt?.outcome, "lease_expired");
    assert.equal(recoveredAttempt?.finished_at, T0 + 10);
    assert.match(String(recoveredAttempt?.error_json), /LeaseExpiredError/);

    await tickLeases(q);
    assertDatabaseInvariants(tmp.path, T0 + 10);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

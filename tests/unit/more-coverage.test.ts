import assert from "node:assert/strict";
import test from "node:test";
import { createFrozenClock } from "../../src/clock.ts";
import { isSqliteBusy, deserializeInternalError, LeaseExpiredError } from "../../src/errors.ts";
import { assertBackoff, delayAfterFailedAttempt } from "../../src/retry/backoff.ts";
import { backoffFromColumns, backoffToColumns } from "../../src/storage/protocol.ts";
import { DurableQValidationError } from "../../src/errors.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

test("frozen clock set and advance", () => {
  const c = createFrozenClock(10);
  assert.equal(c.now(), 10);
  c.advance(5);
  assert.equal(c.now(), 15);
  c.set(100);
  assert.equal(c.now(), 100);
});

test("isSqliteBusy detects locked messages", () => {
  assert.equal(isSqliteBusy({ message: "SQLITE_BUSY" }), true);
  assert.equal(isSqliteBusy({ errcode: 5 }), true);
  assert.equal(isSqliteBusy({ message: "nope" }), false);
  assert.equal(isSqliteBusy(null), false);
});

test("deserialize unknown error names", () => {
  const err = deserializeInternalError({ name: "Nope", message: "x", code: "STORAGE" });
  assert.equal(err.name, "DurableQError");
  const lease = new LeaseExpiredError("gone");
  assert.equal(lease.code, "LEASE_EXPIRED");
});

test("backoff column round-trip and assertBackoff", () => {
  assert.deepEqual(backoffToColumns({ type: "none" }), {
    backoff_type: "none",
    backoff_delay_ms: 0,
    backoff_max_delay_ms: null,
  });
  assert.deepEqual(backoffToColumns({ type: "fixed", delayMs: 3 }), {
    backoff_type: "fixed",
    backoff_delay_ms: 3,
    backoff_max_delay_ms: null,
  });
  const exp = backoffToColumns({ type: "exponential", delayMs: 2, maxDelayMs: 8 });
  assert.equal(
    backoffFromColumns(exp.backoff_type, exp.backoff_delay_ms, exp.backoff_max_delay_ms).type,
    "exponential",
  );
  assert.equal(backoffFromColumns("fixed", 4, null).type, "fixed");
  assertBackoff({ type: "none" });
  assertBackoff({ type: "fixed", delayMs: 1 });
  assert.equal(delayAfterFailedAttempt({ type: "exponential", delayMs: 0, maxDelayMs: 10 }, 3), 0);
});

test("queue get/clean/retry validation and manual retry", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await assert.rejects(() => q.get(""), DurableQValidationError);
    await assert.rejects(
      () => q.clean({ status: "completed", before: 1.5 }),
      DurableQValidationError,
    );
    const { job } = await q.add("n", { v: 1 }, { attempts: 1 });
    const queuedRetry = await q.retry(job.id);
    assert.equal(queuedRetry.retried, false);
    const client = await q.storage();
    await client.request("claim", {
      nowMs: Date.now(),
      queue: q.name,
      workerId: "w",
      names: ["n"],
      leaseDurationMs: 30_000,
    });
    await client.request("settleFailure", {
      nowMs: Date.now(),
      id: job.id,
      workerId: "w",
      generation: 1,
      error: { name: "Error", message: "x" },
      nonRetryable: true,
    });
    const retried = await q.retry(job.id);
    assert.equal(retried.retried, true);
    const missing = await q.retry("nope");
    assert.equal(missing.retried, false);
    await q.add("later", { v: 2 }, { delayMs: 60_000 });
    const hint = await client.request("nextWakeHint", {
      queue: q.name,
      names: ["later"],
      nowMs: Date.now(),
    });
    assert.ok(typeof hint === "number");
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

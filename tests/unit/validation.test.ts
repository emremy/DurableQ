import assert from "node:assert/strict";
import test from "node:test";
import { DurableQValidationError } from "../../src/errors.ts";
import {
  assertLeaseTiming,
  normalizeAddOptions,
  normalizeQueueOptions,
  normalizeWorkerOptions,
} from "../../src/validation.ts";

test("queue options reject memory db and empty path", () => {
  assert.throws(() => normalizeQueueOptions({ path: "" }), DurableQValidationError);
  assert.throws(() => normalizeQueueOptions({ path: ":memory:" }), DurableQValidationError);
  const n = normalizeQueueOptions({ path: "./data/durableq.db" });
  assert.equal(n.busyTimeoutMs, 5000);
});

test("add options defaults and bounds", () => {
  const n = normalizeAddOptions(undefined);
  assert.equal(n.attempts, 3);
  assert.equal(n.priority, 0);
  assert.equal(n.backoff.type, "none");
  assert.throws(() => normalizeAddOptions({ attempts: 0 }), DurableQValidationError);
  assert.throws(() => normalizeAddOptions({ priority: 2_000_000 }), DurableQValidationError);
  assert.throws(() => normalizeAddOptions({ dedupeKey: "" }), DurableQValidationError);
});

test("D-003 lease timing: L > H + B", () => {
  assert.doesNotThrow(() =>
    assertLeaseTiming({
      leaseDurationMs: 30_000,
      heartbeatIntervalMs: 10_000,
      busyTimeoutMs: 5_000,
    }),
  );
  assert.throws(
    () => assertLeaseTiming({ leaseDurationMs: 100, heartbeatIntervalMs: 80, busyTimeoutMs: 50 }),
    DurableQValidationError,
  );
  assert.throws(
    () =>
      assertLeaseTiming({
        leaseDurationMs: 15_000,
        heartbeatIntervalMs: 10_000,
        busyTimeoutMs: 5_000,
      }),
    DurableQValidationError,
  );
});

test("worker options use queue busyTimeout", () => {
  const n = normalizeWorkerOptions(undefined, 5_000);
  assert.equal(n.leaseDurationMs, 30_000);
  assert.equal(n.heartbeatIntervalMs, 10_000);
  assert.throws(() => normalizeWorkerOptions({ concurrency: 0 }, 5_000), DurableQValidationError);
  assert.throws(
    () => normalizeWorkerOptions({ leaseDurationMs: 50, heartbeatIntervalMs: 10 }, 50),
    DurableQValidationError,
  );
});

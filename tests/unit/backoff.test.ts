import assert from "node:assert/strict";
import test from "node:test";
import { delayAfterFailedAttempt, assertBackoff } from "../../src/retry/backoff.ts";
import { DurableQValidationError } from "../../src/errors.ts";

test("none backoff is zero", () => {
  assert.equal(delayAfterFailedAttempt({ type: "none" }, 1), 0);
  assert.equal(delayAfterFailedAttempt({ type: "none" }, 9), 0);
});

test("fixed backoff", () => {
  assert.equal(delayAfterFailedAttempt({ type: "fixed", delayMs: 1000 }, 1), 1000);
  assert.equal(delayAfterFailedAttempt({ type: "fixed", delayMs: 1000 }, 5), 1000);
  assert.equal(delayAfterFailedAttempt({ type: "fixed", delayMs: 0 }, 2), 0);
});

test("exponential attempt 1..N and cap", () => {
  const b = { type: "exponential" as const, delayMs: 1000, maxDelayMs: 60_000 };
  assert.equal(delayAfterFailedAttempt(b, 1), 1000);
  assert.equal(delayAfterFailedAttempt(b, 2), 2000);
  assert.equal(delayAfterFailedAttempt(b, 3), 4000);
  assert.equal(delayAfterFailedAttempt(b, 7), 60_000);
});

test("overflow protection", () => {
  const b = { type: "exponential" as const, delayMs: 1_000_000, maxDelayMs: 5_000_000 };
  assert.equal(delayAfterFailedAttempt(b, 80), 5_000_000);
});

test("invalid values", () => {
  assert.throws(() => delayAfterFailedAttempt({ type: "none" }, 0), DurableQValidationError);
  assert.throws(
    () => delayAfterFailedAttempt({ type: "fixed", delayMs: -1 }, 1),
    DurableQValidationError,
  );
  assert.throws(
    () => delayAfterFailedAttempt({ type: "exponential", delayMs: 10, maxDelayMs: 1 }, 1),
    DurableQValidationError,
  );
  assert.throws(
    () => assertBackoff({ type: "exponential", delayMs: 10, maxDelayMs: 1 }),
    DurableQValidationError,
  );
});

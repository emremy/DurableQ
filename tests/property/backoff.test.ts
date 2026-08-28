import assert from "node:assert/strict";
import test from "node:test";
import * as fc from "fast-check";
import { delayAfterFailedAttempt } from "../../src/retry/backoff.ts";

test("property: exponential delay is finite, monotonic until cap, never NaN", () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 0, max: 10_000 }),
      fc.integer({ min: 0, max: 1_000_000 }),
      fc.integer({ min: 1, max: 20 }),
      (delayMs, extra, n) => {
        const maxDelayMs = delayMs + extra;
        const d = delayAfterFailedAttempt({ type: "exponential", delayMs, maxDelayMs }, n);
        assert.equal(Number.isFinite(d), true);
        assert.ok(d >= 0);
        assert.ok(d <= maxDelayMs);
        const next = delayAfterFailedAttempt({ type: "exponential", delayMs, maxDelayMs }, n + 1);
        assert.ok(next >= d || next === maxDelayMs);
      },
    ),
    { numRuns: 40 },
  );
});

test("property: fixed delay is constant", () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 0, max: 10_000 }),
      fc.integer({ min: 1, max: 20 }),
      (delayMs, n) => {
        assert.equal(delayAfterFailedAttempt({ type: "fixed", delayMs }, n), delayMs);
      },
    ),
    { numRuns: 20 },
  );
});

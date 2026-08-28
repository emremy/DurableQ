import assert from "node:assert/strict";
import test from "node:test";
import { MAX_HANDLERS_PER_WORKER, MAX_NAME_LENGTH } from "../../src/constants.ts";
import { DurableQValidationError } from "../../src/errors.ts";
import { Queue } from "../../src/queue.ts";
import { Worker } from "../../src/worker.ts";
import {
  assertHandlerCount,
  assertJobNameList,
  assertName,
  assertSafeInteger,
  normalizeAddOptions,
} from "../../src/validation.ts";
import { assertBackoff, delayAfterFailedAttempt } from "../../src/retry/backoff.ts";
import { decodeJson, encodeJson, assertJsonValue } from "../../src/codec/json.ts";
import { serializeStoredError, storedErrorToJson } from "../../src/codec/error.ts";
import { DurableQSerializationError } from "../../src/errors.ts";

test("assertName and handler list edges", () => {
  assert.throws(() => assertName("", "n"), DurableQValidationError);
  assert.throws(() => assertName("x".repeat(MAX_NAME_LENGTH + 1), "n"), DurableQValidationError);
  assert.throws(() => assertName("a\0b", "n"), DurableQValidationError);
  assertName("ok", "n");
  assertName("x".repeat(MAX_NAME_LENGTH), "n");
  assert.throws(() => assertHandlerCount(0), DurableQValidationError);
  assert.doesNotThrow(() => assertHandlerCount(1));
  assert.throws(() => assertHandlerCount(MAX_HANDLERS_PER_WORKER + 1), DurableQValidationError);
  assert.doesNotThrow(() => assertHandlerCount(MAX_HANDLERS_PER_WORKER));
  assert.throws(() => assertJobNameList([]), DurableQValidationError);
  assert.throws(() => assertJobNameList(["a", "a"]), DurableQValidationError);
  assert.doesNotThrow(() => assertJobNameList(["a", "b"]));
  assert.throws(() => assertSafeInteger(1.5, "x", 0, 10), DurableQValidationError);
  assert.throws(() => assertSafeInteger(-1, "x", 0, 10), DurableQValidationError);
});

test("Worker rejects empty and oversized handler maps", () => {
  const q = new Queue("app", { path: "/tmp/durableq-validation.db", busyTimeoutMs: 50 });
  assert.throws(() => new Worker(q, {}), DurableQValidationError);
  const tooMany = Object.fromEntries(
    Array.from({ length: MAX_HANDLERS_PER_WORKER + 1 }, (_, i) => [
      `h${i}`,
      async () => ({ ok: true }),
    ]),
  );
  assert.throws(() => new Worker(q, tooMany), DurableQValidationError);
});

test("add option remaining bounds", () => {
  assert.throws(() => normalizeAddOptions({ delayMs: -1 }), DurableQValidationError);
  assert.throws(() => normalizeAddOptions({ attemptTimeoutMs: 0 }), DurableQValidationError);
  assert.throws(() => normalizeAddOptions({ dedupeKey: "a\0b" }), DurableQValidationError);
  assert.throws(() => normalizeAddOptions({ dedupeKey: "k".repeat(300) }), DurableQValidationError);
  const n = normalizeAddOptions({ attemptTimeoutMs: 10, dedupeKey: "k".repeat(256) });
  assert.equal(n.attemptTimeoutMs, 10);
  assert.equal(n.dedupeKey?.length, 256);
});

test("backoff remaining edges", () => {
  assert.equal(delayAfterFailedAttempt({ type: "exponential", delayMs: 0, maxDelayMs: 10 }, 2), 0);
  assert.equal(delayAfterFailedAttempt({ type: "exponential", delayMs: 4, maxDelayMs: 4 }, 1), 4);
  assert.throws(() => delayAfterFailedAttempt({ type: "none" }, 1.2), DurableQValidationError);
  assert.throws(
    () => assertBackoff({ type: "nope" } as unknown as { type: "none" }),
    DurableQValidationError,
  );
  assertBackoff({ type: "fixed", delayMs: 0 });
});

test("json nested illegal values and sparse arrays", () => {
  assert.throws(() => encodeJson([undefined], 1024), DurableQSerializationError);
  assert.throws(() => encodeJson({ a: 1n }, 1024), DurableQSerializationError);
  assert.throws(() => encodeJson({ a: new Date() }, 1024), DurableQSerializationError);
  assert.throws(() => encodeJson({ a: new Map() }, 1024), DurableQSerializationError);
  assert.throws(() => encodeJson({ a: new Set() }, 1024), DurableQSerializationError);
  assert.throws(() => encodeJson(new ArrayBuffer(8), 1024), DurableQSerializationError);
  assert.throws(() => encodeJson(new WeakSet(), 1024), DurableQSerializationError);
  assert.throws(() => encodeJson(new Float32Array(2), 1024), DurableQSerializationError);
  const sparse = [] as unknown[];
  sparse[1] = 1;
  assert.throws(() => encodeJson(sparse, 1024), DurableQSerializationError);
  assert.throws(() => decodeJson("{"), DurableQSerializationError);
  assert.throws(() => encodeJson(Number.NaN, 8), DurableQSerializationError);
  assertJsonValue(null);
  assert.deepEqual(decodeJson("0"), 0);
});

test("stored error non-Error and tiny budget", () => {
  const stored = serializeStoredError("plain", 64);
  assert.equal(stored.name, "Error");
  assert.equal(stored.message, "plain");
  const withStack = new Error("m");
  const s1 = serializeStoredError(withStack);
  assert.equal(typeof s1.stack, "string");
  const numCode = Object.assign(new Error("m"), { code: 42 });
  assert.equal(serializeStoredError(numCode).code, 42);
  const nanCode = Object.assign(new Error("m"), { code: Number.NaN });
  const s2 = serializeStoredError(nanCode);
  assert.equal("code" in s2, false);
  const json = storedErrorToJson({ name: "E", message: "x".repeat(200) }, 80);
  assert.ok(Buffer.byteLength(json, "utf8") <= 80);
});

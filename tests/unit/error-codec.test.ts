import assert from "node:assert/strict";
import test from "node:test";
import { serializeStoredError, storedErrorToJson } from "../../src/codec/error.ts";

test("error truncation is deterministic", () => {
  const err = new Error("x".repeat(100_000));
  err.name = "Boom";
  const a = serializeStoredError(err, 256);
  const b = serializeStoredError(err, 256);
  assert.deepEqual(a, b);
  assert.ok(Buffer.byteLength(JSON.stringify(a), "utf8") <= 256);
});

test("bounded fields only", () => {
  const err = Object.assign(new Error("msg"), { extra: { nested: true }, code: "E" });
  const stored = serializeStoredError(err);
  assert.equal(stored.name, "Error");
  assert.equal(stored.message, "msg");
  assert.equal(stored.code, "E");
  assert.equal("extra" in stored, false);
});

test("storedErrorToJson preserves StoredError name", () => {
  const json = storedErrorToJson({ name: "LeaseExpiredError", message: "lease expired" });
  assert.match(json, /LeaseExpiredError/);
  assert.match(json, /lease expired/);
});

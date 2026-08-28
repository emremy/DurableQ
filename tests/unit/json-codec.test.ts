import assert from "node:assert/strict";
import test from "node:test";
import { encodeJson, decodeJson, assertJsonValue } from "../../src/codec/json.ts";
import { DurableQSerializationError } from "../../src/errors.ts";

const LIMIT = 1024;

test("INV-014 primitives round-trip", () => {
  for (const v of ["", "hello", 0, 1, -2, true, false, null, "unicode ✓ 𝄞"]) {
    assert.deepEqual(decodeJson(encodeJson(v, LIMIT)), v);
  }
});

test("nested objects and arrays round-trip", () => {
  const v = { a: [1, { b: "x" }], c: [] as number[] };
  assert.deepEqual(decodeJson(encodeJson(v, LIMIT)), v);
});

test("empty object and array", () => {
  assert.deepEqual(decodeJson(encodeJson({}, LIMIT)), {});
  assert.deepEqual(decodeJson(encodeJson([], LIMIT)), []);
});

test("rejects undefined root and nested", () => {
  assert.throws(() => encodeJson(undefined, LIMIT), DurableQSerializationError);
  assert.throws(() => encodeJson({ a: undefined }, LIMIT), DurableQSerializationError);
});

test("rejects bigint, function, symbol", () => {
  assert.throws(() => encodeJson(1n, LIMIT), DurableQSerializationError);
  assert.throws(() => encodeJson(() => 1, LIMIT), DurableQSerializationError);
  assert.throws(() => encodeJson(Symbol("x"), LIMIT), DurableQSerializationError);
});

test("rejects NaN and Infinity", () => {
  assert.throws(() => encodeJson(Number.NaN, LIMIT), DurableQSerializationError);
  assert.throws(() => encodeJson(Infinity, LIMIT), DurableQSerializationError);
  assert.throws(() => encodeJson(-Infinity, LIMIT), DurableQSerializationError);
});

test("rejects Date, Map, Set, class, buffer", () => {
  assert.throws(() => encodeJson(new Date(), LIMIT), DurableQSerializationError);
  assert.throws(() => encodeJson(new Map(), LIMIT), DurableQSerializationError);
  assert.throws(() => encodeJson(new Set(), LIMIT), DurableQSerializationError);
  class C {}
  assert.throws(() => encodeJson(new C(), LIMIT), DurableQSerializationError);
  assert.throws(() => encodeJson(Buffer.from("x"), LIMIT), DurableQSerializationError);
});

test("rejects cyclic reference", () => {
  const a: { self?: unknown } = {};
  a.self = a;
  assert.throws(() => encodeJson(a, LIMIT), DurableQSerializationError);
});

test("payload byte limit exact boundary", () => {
  const s = "a".repeat(10);
  const encoded = encodeJson(s, Buffer.byteLength(JSON.stringify(s), "utf8"));
  assert.equal(decodeJson(encoded), s);
  assert.throws(
    () => encodeJson(s, Buffer.byteLength(JSON.stringify(s), "utf8") - 1),
    DurableQSerializationError,
  );
});

test("Object.create(null) is allowed", () => {
  const o = Object.create(null) as Record<string, number>;
  o.a = 1;
  assert.deepEqual(decodeJson(encodeJson(o, LIMIT)), { a: 1 });
});

test("assertJsonValue is the same contract", () => {
  assert.doesNotThrow(() => assertJsonValue({ a: [true, null] }));
  assert.throws(() => assertJsonValue({ a: undefined }), DurableQSerializationError);
});

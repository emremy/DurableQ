import assert from "node:assert/strict";
import test from "node:test";
import * as fc from "fast-check";
import { decodeJson, encodeJson } from "../../src/codec/json.ts";
import { DurableQSerializationError } from "../../src/errors.ts";

const jsonArb: fc.Arbitrary<unknown> = fc.letrec((tie) => ({
  json: fc.oneof(
    { depthSize: "small" },
    fc.constant(null),
    fc.boolean(),
    fc.string(),
    fc.integer({ min: -1e9, max: 1e9 }),
    fc.double({ noNaN: true, noDefaultInfinity: true }).filter((n) => Number.isFinite(n)),
    fc.array(tie("json"), { maxLength: 4 }),
    fc.dictionary(
      fc.string().filter((s) => s !== "__proto__"),
      tie("json"),
      { maxKeys: 4 },
    ),
  ),
})).json;

test("property: encode/decode round-trip for JSON-safe values", () => {
  fc.assert(
    fc.property(jsonArb, (value) => {
      const encoded = encodeJson(value, 1_000_000);
      assert.deepEqual(decodeJson(encoded), JSON.parse(encoded));
    }),
    { numRuns: 50 },
  );
});

test("property: forbidden values are rejected", () => {
  const forbidden = [undefined, 1n, Number.NaN, Infinity, -Infinity, new Date(), () => 0];
  for (const v of forbidden) {
    assert.throws(() => encodeJson(v, 1000), DurableQSerializationError);
  }
});

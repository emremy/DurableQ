import assert from "node:assert/strict";
import test from "node:test";
import {
  DurableQBusyError,
  DurableQStaleLeaseError,
  deserializeInternalError,
  serializeInternalError,
} from "../../src/errors.ts";
import { isStorageRequest } from "../../src/storage/protocol.ts";

test("RPC error round-trip preserves class", () => {
  const err = new DurableQStaleLeaseError("stale");
  const again = deserializeInternalError(serializeInternalError(err));
  assert.equal(again.name, "DurableQStaleLeaseError");
  assert.equal(again.message, "stale");
  assert.ok(again instanceof DurableQStaleLeaseError);
});

test("busy error class", () => {
  const err = new DurableQBusyError("locked");
  const again = deserializeInternalError(serializeInternalError(err));
  assert.ok(again instanceof DurableQBusyError);
});

test("malformed storage request rejected by guard", () => {
  assert.equal(isStorageRequest(null), false);
  assert.equal(isStorageRequest({}), false);
  assert.equal(isStorageRequest({ id: "1", op: "get" }), true);
});

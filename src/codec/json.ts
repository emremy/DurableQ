import { DurableQSerializationError } from "../errors.ts";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { readonly [key: string]: JsonValue };

function fail(message: string): never {
  throw new DurableQSerializationError(message);
}

function isTypedArray(value: object): boolean {
  return ArrayBuffer.isView(value);
}

export function assertJsonValue(value: unknown, path = "$"): asserts value is JsonValue {
  if (value === null) return;
  const t = typeof value;
  if (t === "string" || t === "boolean") return;
  if (t === "number") {
    if (!Number.isFinite(value)) fail(`${path}: numbers must be finite (no NaN/Infinity)`);
    return;
  }
  if (t === "undefined") fail(`${path}: undefined is not JSON-safe`);
  if (t === "bigint") fail(`${path}: bigint is not JSON-safe`);
  if (t === "symbol") fail(`${path}: symbol is not JSON-safe`);
  if (t === "function") fail(`${path}: functions are not JSON-safe`);
  if (t !== "object") fail(`${path}: unsupported type ${t}`);

  const obj = value as object;
  if (obj instanceof Date) fail(`${path}: Date is not JSON-safe; convert explicitly`);
  if (obj instanceof Map) fail(`${path}: Map is not JSON-safe; convert explicitly`);
  if (obj instanceof Set) fail(`${path}: Set is not JSON-safe; convert explicitly`);
  if (obj instanceof WeakMap || obj instanceof WeakSet)
    fail(`${path}: weak collections are not JSON-safe`);
  if (isTypedArray(obj) || obj instanceof ArrayBuffer) {
    fail(`${path}: binary types are not JSON-safe; convert explicitly`);
  }

  const seen = new WeakSet<object>();
  walk(obj, path, seen);
}

function walk(value: object, path: string, seen: WeakSet<object>): void {
  if (seen.has(value)) fail(`${path}: cyclic reference`);
  seen.add(value);

  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      if (!Object.hasOwn(value, i)) fail(`${path}[${i}]: sparse arrays are not JSON-safe`);
      assertChild(value[i], `${path}[${i}]`, seen);
    }
    return;
  }

  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) {
    fail(`${path}: class instances are not JSON-safe; convert to a plain object`);
  }

  for (const key of Object.keys(value)) {
    const child = (value as Record<string, unknown>)[key];
    assertChild(child, `${path}.${key}`, seen);
  }
}

function assertChild(value: unknown, path: string, seen: WeakSet<object>): void {
  if (value === null) return;
  const t = typeof value;
  if (t === "string" || t === "boolean") return;
  if (t === "number") {
    if (!Number.isFinite(value)) fail(`${path}: numbers must be finite (no NaN/Infinity)`);
    return;
  }
  if (t === "undefined") fail(`${path}: undefined is not JSON-safe`);
  if (t === "bigint") fail(`${path}: bigint is not JSON-safe`);
  if (t === "symbol") fail(`${path}: symbol is not JSON-safe`);
  if (t === "function") fail(`${path}: functions are not JSON-safe`);
  if (t !== "object") fail(`${path}: unsupported type ${t}`);
  const obj = value as object;
  if (obj instanceof Date) fail(`${path}: Date is not JSON-safe; convert explicitly`);
  if (obj instanceof Map) fail(`${path}: Map is not JSON-safe; convert explicitly`);
  if (obj instanceof Set) fail(`${path}: Set is not JSON-safe; convert explicitly`);
  if (isTypedArray(obj) || obj instanceof ArrayBuffer) {
    fail(`${path}: binary types are not JSON-safe; convert explicitly`);
  }
  walk(obj, path, seen);
}

export function encodeJson(value: unknown, maxBytes: number): string {
  assertJsonValue(value);
  let encoded: string;
  try {
    encoded = JSON.stringify(value);
  } catch (cause) {
    throw new DurableQSerializationError("JSON stringify failed", { cause });
  }
  if (typeof encoded !== "string") fail("JSON.stringify produced a non-string");
  const bytes = Buffer.byteLength(encoded, "utf8");
  if (bytes > maxBytes) {
    fail(`JSON payload is ${bytes} bytes; limit is ${maxBytes}`);
  }
  return encoded;
}

export function decodeJson(text: string): JsonValue {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (cause) {
    throw new DurableQSerializationError("stored JSON is malformed", { cause });
  }
  assertJsonValue(parsed);
  return parsed;
}

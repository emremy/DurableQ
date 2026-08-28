import { MAX_STORED_ERROR_BYTES } from "../constants.ts";
import type { StoredError } from "../types.ts";

function asCode(value: unknown): string | number | undefined {
  if (typeof value === "string" || typeof value === "number") {
    if (typeof value === "number" && !Number.isFinite(value)) return undefined;
    return value;
  }
  return undefined;
}

function clip(text: string, maxChars: number): string {
  if (text.length <= maxChars) return text;
  return text.slice(0, Math.max(0, maxChars - 1)) + "…";
}

export function serializeStoredError(err: unknown, maxBytes = MAX_STORED_ERROR_BYTES): StoredError {
  const name = err instanceof Error ? err.name : "Error";
  const message = err instanceof Error ? err.message : String(err);
  const stack = err instanceof Error ? err.stack : undefined;
  const code =
    err && typeof err === "object" && "code" in err
      ? asCode((err as { code: unknown }).code)
      : undefined;

  const base: StoredError = { name: clip(name, 256), message: clip(message, 8_192) };
  if (code !== undefined) base.code = code;
  if (typeof stack === "string") base.stack = stack;

  let encoded = JSON.stringify(base);
  if (Buffer.byteLength(encoded, "utf8") <= maxBytes) {
    return JSON.parse(encoded) as StoredError;
  }

  const shrunk: StoredError = { name: base.name, message: clip(base.message, 1_024) };
  if (base.code !== undefined) shrunk.code = base.code;
  encoded = JSON.stringify(shrunk);
  while (Buffer.byteLength(encoded, "utf8") > maxBytes && shrunk.message.length > 0) {
    shrunk.message = clip(shrunk.message, Math.max(0, shrunk.message.length - 32));
    encoded = JSON.stringify(shrunk);
  }
  return JSON.parse(encoded) as StoredError;
}

export function storedErrorToJson(err: StoredError, maxBytes = MAX_STORED_ERROR_BYTES): string {
  const json = JSON.stringify(serializeStoredError(err, maxBytes));
  if (Buffer.byteLength(json, "utf8") > maxBytes) {
    return JSON.stringify(serializeStoredError(err, maxBytes));
  }
  return json;
}

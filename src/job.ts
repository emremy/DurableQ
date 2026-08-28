import { decodeJson } from "./codec/json.ts";
import type { JobRow } from "./storage/protocol.ts";
import { backoffFromColumns } from "./storage/protocol.ts";
import type { JobView, StoredError } from "./types.ts";

function parseStoredError(text: string | null): StoredError | null {
  if (text === null) return null;
  const value = decodeJson(text);
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const rec = value as Record<string, unknown>;
  const out: StoredError = {
    name: typeof rec.name === "string" ? rec.name : "Error",
    message: typeof rec.message === "string" ? rec.message : "",
  };
  if (typeof rec.stack === "string") out.stack = rec.stack;
  if (typeof rec.code === "string" || typeof rec.code === "number") out.code = rec.code;
  return out;
}

export function jobRowToView(row: JobRow): JobView {
  return {
    id: row.id,
    queue: row.queue,
    name: row.name,
    data: decodeJson(row.payload_json),
    status: row.status,
    priority: row.priority,
    availableAt: row.available_at,
    attemptCount: row.attempt_count,
    maxAttempts: row.max_attempts,
    backoff: backoffFromColumns(row.backoff_type, row.backoff_delay_ms, row.backoff_max_delay_ms),
    attemptTimeoutMs: row.attempt_timeout_ms,
    leaseOwner: row.lease_owner,
    leaseGeneration: row.lease_generation,
    leaseExpiresAt: row.lease_expires_at,
    dedupeKey: row.dedupe_key,
    result: row.result_json === null ? null : decodeJson(row.result_json),
    error: parseStoredError(row.error_json),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    failedAt: row.failed_at,
    cancelledAt: row.cancelled_at,
  };
}

export function sqliteRowToJobRow(raw: Record<string, unknown>): JobRow {
  return {
    id: String(raw.id),
    queue: String(raw.queue),
    name: String(raw.name),
    payload_json: String(raw.payload_json),
    status: raw.status as JobRow["status"],
    priority: Number(raw.priority),
    available_at: Number(raw.available_at),
    attempt_count: Number(raw.attempt_count),
    max_attempts: Number(raw.max_attempts),
    backoff_type: raw.backoff_type as JobRow["backoff_type"],
    backoff_delay_ms: Number(raw.backoff_delay_ms),
    backoff_max_delay_ms:
      raw.backoff_max_delay_ms === null ? null : Number(raw.backoff_max_delay_ms),
    attempt_timeout_ms: raw.attempt_timeout_ms === null ? null : Number(raw.attempt_timeout_ms),
    lease_owner: raw.lease_owner === null ? null : String(raw.lease_owner),
    lease_generation: Number(raw.lease_generation),
    lease_expires_at: raw.lease_expires_at === null ? null : Number(raw.lease_expires_at),
    dedupe_key: raw.dedupe_key === null ? null : String(raw.dedupe_key),
    result_json:
      raw.result_json === null || raw.result_json === undefined ? null : String(raw.result_json),
    error_json:
      raw.error_json === null || raw.error_json === undefined ? null : String(raw.error_json),
    created_at: Number(raw.created_at),
    updated_at: Number(raw.updated_at),
    started_at: raw.started_at === null ? null : Number(raw.started_at),
    completed_at: raw.completed_at === null ? null : Number(raw.completed_at),
    failed_at: raw.failed_at === null ? null : Number(raw.failed_at),
    cancelled_at: raw.cancelled_at === null ? null : Number(raw.cancelled_at),
  };
}

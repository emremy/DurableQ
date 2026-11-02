import type { SerializedInternalError } from "../errors.ts";
import type { BackoffConfig, JobStatus, JsonValue, StoredError } from "../types.ts";

export type StorageOp =
  | "init"
  | "enqueue"
  | "enqueueMany"
  | "get"
  | "count"
  | "claim"
  | "heartbeat"
  | "settleSuccess"
  | "settleFailure"
  | "manualRetry"
  | "cancelQueued"
  | "clean"
  | "updateProgress"
  | "setPaused"
  | "isPaused"
  | "stats"
  | "list"
  | "upsertSchedule"
  | "getSchedule"
  | "listSchedules"
  | "removeSchedule"
  | "applyRetention"
  | "recoverExpired"
  | "startLeaseTracking"
  | "stopLeaseTracking"
  | "nextWakeHint"
  | "close";

export type StorageRequest = {
  id: string;
  op: string;
  payload: unknown;
};

export type StorageSuccess = {
  id: string;
  ok: true;
  result: unknown;
};

export type StorageFailure = {
  id: string;
  ok: false;
  error: SerializedInternalError;
};

export type StorageResponse = StorageSuccess | StorageFailure;

export type WorkerEventMessage = {
  type: "event";
  event: "attemptTimeout" | "leaseLost";
  jobId: string;
  generation: number;
};

export type JobRow = {
  id: string;
  queue: string;
  name: string;
  payload_json: string;
  status: JobStatus;
  priority: number;
  available_at: number;
  attempt_count: number;
  max_attempts: number;
  backoff_type: "none" | "fixed" | "exponential";
  backoff_delay_ms: number;
  backoff_max_delay_ms: number | null;
  attempt_timeout_ms: number | null;
  lease_owner: string | null;
  lease_generation: number;
  lease_expires_at: number | null;
  dedupe_key: string | null;
  result_json: string | null;
  error_json: string | null;
  created_at: number;
  updated_at: number;
  started_at: number | null;
  completed_at: number | null;
  failed_at: number | null;
  cancelled_at: number | null;
  progress: number;
};

export type ScheduleRow = {
  queue: string;
  id: string;
  name: string;
  payload_json: string;
  every_ms: number;
  next_run_at: number;
  max_attempts: number;
  priority: number;
  attempt_timeout_ms: number | null;
  backoff_type: "none" | "fixed" | "exponential";
  backoff_delay_ms: number;
  backoff_max_delay_ms: number | null;
  last_job_id: string | null;
  created_at: number;
  updated_at: number;
};

export type EnqueuePayload = {
  nowMs: number;
  queue: string;
  name: string;
  payloadJson: string;
  maxAttempts: number;
  delayMs: number;
  priority: number;
  dedupeKey: string | null;
  attemptTimeoutMs: number | null;
  backoff: BackoffConfig;
};

export type UpsertSchedulePayload = {
  nowMs: number;
  queue: string;
  id: string;
  name: string;
  payloadJson: string;
  everyMs: number;
  maxAttempts: number;
  priority: number;
  attemptTimeoutMs: number | null;
  backoff: BackoffConfig;
};

export type ProgressPayload = {
  nowMs: number;
  id: string;
  workerId: string;
  generation: number;
  progress: number;
};

export type ListPayload = {
  queue: string;
  status: JobStatus | undefined;
  name: string | undefined;
  limit: number;
  cursor: { updatedAt: number; rowid: number } | null;
};

export type RetentionPayload = {
  nowMs: number;
  queue: string;
  rules: { status: "completed" | "failed" | "cancelled"; ageMs: number }[];
};

export type ClaimPayload = {
  nowMs: number;
  queue: string;
  workerId: string;
  names: string[];
  leaseDurationMs: number;
};

export type SettleSuccessPayload = {
  nowMs: number;
  id: string;
  workerId: string;
  generation: number;
  resultJson: string;
};

export type SettleFailurePayload = {
  nowMs: number;
  id: string;
  workerId: string;
  generation: number;
  error: StoredError;
  nonRetryable: boolean;
};

export type HeartbeatPayload = {
  nowMs: number;
  id: string;
  workerId: string;
  generation: number;
  leaseDurationMs: number;
};

export type TrackPayload = {
  jobId: string;
  workerId: string;
  generation: number;
  leaseDurationMs: number;
  heartbeatIntervalMs: number;
  attemptDeadlineMs: number | null;
};

export function isStorageRequest(value: unknown): value is StorageRequest {
  if (!value || typeof value !== "object") return false;
  const v = value as Record<string, unknown>;
  return typeof v.id === "string" && typeof v.op === "string";
}

export function backoffToColumns(backoff: BackoffConfig): {
  backoff_type: "none" | "fixed" | "exponential";
  backoff_delay_ms: number;
  backoff_max_delay_ms: number | null;
} {
  if (backoff.type === "none") {
    return { backoff_type: "none", backoff_delay_ms: 0, backoff_max_delay_ms: null };
  }
  if (backoff.type === "fixed") {
    return { backoff_type: "fixed", backoff_delay_ms: backoff.delayMs, backoff_max_delay_ms: null };
  }
  return {
    backoff_type: "exponential",
    backoff_delay_ms: backoff.delayMs,
    backoff_max_delay_ms: backoff.maxDelayMs,
  };
}

export function backoffFromColumns(
  type: string,
  delayMs: number,
  maxDelayMs: number | null,
): BackoffConfig {
  if (type === "fixed") return { type: "fixed", delayMs };
  if (type === "exponential")
    return { type: "exponential", delayMs, maxDelayMs: maxDelayMs ?? delayMs };
  return { type: "none" };
}

export { type JsonValue };

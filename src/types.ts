import type { JOB_STATUSES } from "./constants.ts";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { readonly [key: string]: JsonValue };

export type JobStatus = (typeof JOB_STATUSES)[number];

export type BackoffConfig =
  | { type: "none" }
  | { type: "fixed"; delayMs: number }
  | { type: "exponential"; delayMs: number; maxDelayMs: number };

export type JobDefinition = {
  input: JsonValue;
  output: JsonValue;
};

export type JobMap = Record<string, JobDefinition>;

export type QueueOptions = {
  path: string;
  busyTimeoutMs?: number;
  maxPayloadBytes?: number;
  maxResultBytes?: number;
};

export type AddOptions = {
  attempts?: number;
  delayMs?: number;
  priority?: number;
  dedupeKey?: string;
  attemptTimeoutMs?: number;
  backoff?: BackoffConfig;
};

export type JobView<Name extends string = string, Data extends JsonValue = JsonValue> = {
  id: string;
  queue: string;
  name: Name;
  data: Data;
  status: JobStatus;
  priority: number;
  availableAt: number;
  attemptCount: number;
  maxAttempts: number;
  backoff: BackoffConfig;
  attemptTimeoutMs: number | null;
  leaseOwner: string | null;
  leaseGeneration: number;
  leaseExpiresAt: number | null;
  dedupeKey: string | null;
  result: JsonValue | null;
  error: StoredError | null;
  createdAt: number;
  updatedAt: number;
  startedAt: number | null;
  completedAt: number | null;
  failedAt: number | null;
  cancelledAt: number | null;
};

export type AddResult<Name extends string = string, Data extends JsonValue = JsonValue> = {
  job: JobView<Name, Data>;
  deduplicated: boolean;
};

export type StoredError = {
  name: string;
  message: string;
  stack?: string;
  code?: string | number;
};

export type JobContext = {
  signal: AbortSignal;
  attempt: number;
  maxAttempts: number;
  workerId: string;
};

export type JobHandler<Input extends JsonValue, Output extends JsonValue> = (
  job: JobView<string, Input>,
  ctx: JobContext,
) => Promise<Output> | Output;

export type WorkerOptions = {
  concurrency?: number;
  pollIntervalMs?: number;
  leaseDurationMs?: number;
  heartbeatIntervalMs?: number;
  defaultAttemptTimeoutMs?: number;
};

export type CancelResult =
  | { cancelled: true }
  | { cancelled: false; reason: "missing" | "active" | "completed" | "failed" | "cancelled" };

export type RetryFailureReason = "missing" | "active" | "queued" | "completed" | "cancelled";

export type RetryResult =
  { retried: true; job: JobView } | { retried: false; reason: RetryFailureReason };

export type CleanOptions = {
  status: "completed" | "failed" | "cancelled";
  before: number;
};

export type CountFilter = {
  status?: JobStatus;
};

export type NormalizedQueueOptions = {
  path: string;
  busyTimeoutMs: number;
  maxPayloadBytes: number;
  maxResultBytes: number;
};

export type NormalizedAddOptions = {
  attempts: number;
  delayMs: number;
  priority: number;
  dedupeKey: string | null;
  attemptTimeoutMs: number | null;
  backoff: BackoffConfig;
};

export type NormalizedWorkerOptions = {
  concurrency: number;
  pollIntervalMs: number;
  leaseDurationMs: number;
  heartbeatIntervalMs: number;
  defaultAttemptTimeoutMs: number;
};

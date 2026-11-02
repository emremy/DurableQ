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

export type RetentionRule = {
  /** Delete jobs this long after they reached the status. */
  ageMs: number;
};

export type RetentionOptions = {
  completed?: RetentionRule;
  failed?: RetentionRule;
  cancelled?: RetentionRule;
};

export type QueueOptions = {
  path: string;
  busyTimeoutMs?: number;
  maxPayloadBytes?: number;
  maxResultBytes?: number;
  /** Automatic cleanup of terminal jobs, run by this queue's workers. Disabled by default. */
  retention?: RetentionOptions;
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
  /** 0–100, reported by the current or last attempt; reset to 0 when an attempt starts. */
  progress: number;
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
  /**
   * Durably records progress (an integer 0–100). Resolves after the value is committed;
   * rejects with DurableQStaleLeaseError if this attempt no longer owns the job.
   */
  updateProgress(progress: number): Promise<void>;
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

export type AddManyItem<Jobs extends JobMap = JobMap> = {
  [K in keyof Jobs & string]: { name: K; data: Jobs[K]["input"]; options?: AddOptions };
}[keyof Jobs & string];

export type ScheduleOptions<Data extends JsonValue = JsonValue> = {
  /** Stable identifier, unique within the queue. */
  id: string;
  /** Fixed interval between occurrences, in milliseconds (>= 1000). */
  everyMs: number;
  data: Data;
  attempts?: number;
  priority?: number;
  attemptTimeoutMs?: number;
  backoff?: BackoffConfig;
};

export type ScheduleView<Name extends string = string, Data extends JsonValue = JsonValue> = {
  id: string;
  queue: string;
  name: Name;
  data: Data;
  everyMs: number;
  nextRunAt: number;
  attempts: number;
  priority: number;
  attemptTimeoutMs: number | null;
  backoff: BackoffConfig;
  lastJobId: string | null;
  createdAt: number;
  updatedAt: number;
};

export type ScheduleResult<Name extends string = string, Data extends JsonValue = JsonValue> = {
  schedule: ScheduleView<Name, Data>;
  outcome: "created" | "updated" | "unchanged";
};

export type QueueStats = {
  queued: number;
  active: number;
  completed: number;
  failed: number;
  cancelled: number;
  /** Queued jobs whose availableAt has passed (subset of queued). */
  due: number;
  /** Queued jobs whose availableAt is in the future (subset of queued; due + delayed = queued). */
  delayed: number;
  /** Queued jobs waiting for another attempt after a failure (subset of queued). */
  retrying: number;
  /** Milliseconds since the oldest due job became available; null when nothing is due. */
  oldestQueuedAgeMs: number | null;
  paused: boolean;
};

export type ListOptions = {
  status?: JobStatus;
  name?: string;
  /** Default 50, maximum 1000. */
  limit?: number;
  /** nextCursor from a previous page. */
  cursor?: string;
};

export type ListResult = {
  jobs: JobView[];
  nextCursor: string | null;
};

export type CountFilter = {
  status?: JobStatus;
};

export type NormalizedQueueOptions = {
  path: string;
  busyTimeoutMs: number;
  maxPayloadBytes: number;
  maxResultBytes: number;
  retention: { status: "completed" | "failed" | "cancelled"; ageMs: number }[];
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

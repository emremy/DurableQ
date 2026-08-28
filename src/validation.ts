import {
  DEFAULT_ATTEMPT_TIMEOUT_MS,
  DEFAULT_BUSY_TIMEOUT_MS,
  DEFAULT_CONCURRENCY,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_LEASE_DURATION_MS,
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_MAX_PAYLOAD_BYTES,
  DEFAULT_MAX_RESULT_BYTES,
  DEFAULT_POLL_INTERVAL_MS,
  DEFAULT_PRIORITY,
  MAX_CONCURRENCY,
  MAX_DEDUPE_KEY_LENGTH,
  MAX_HANDLERS_PER_WORKER,
  MAX_NAME_LENGTH,
  MAX_PRIORITY,
  MIN_CONCURRENCY,
  MIN_PRIORITY,
} from "./constants.ts";
import { DurableQValidationError } from "./errors.ts";
import { assertBackoff } from "./retry/backoff.ts";
import type {
  AddOptions,
  BackoffConfig,
  NormalizedAddOptions,
  NormalizedQueueOptions,
  NormalizedWorkerOptions,
  QueueOptions,
  WorkerOptions,
} from "./types.ts";

export function assertSafeInteger(
  value: unknown,
  label: string,
  min: number,
  max: number,
): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new DurableQValidationError(`${label} must be a safe integer in [${min}, ${max}]`);
  }
}

export function assertName(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0 || value.length > MAX_NAME_LENGTH) {
    throw new DurableQValidationError(
      `${label} must be a non-empty string of at most ${MAX_NAME_LENGTH} characters`,
    );
  }
  if (value.includes("\0")) {
    throw new DurableQValidationError(`${label} must not contain NUL bytes`);
  }
}

export function normalizeQueueOptions(options: QueueOptions): NormalizedQueueOptions {
  if (!options || typeof options.path !== "string" || options.path.length === 0) {
    throw new DurableQValidationError("queue path is required");
  }
  if (options.path === ":memory:") {
    throw new DurableQValidationError("in-memory SQLite is not supported; use a file path");
  }
  const busyTimeoutMs = options.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS;
  const maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
  const maxResultBytes = options.maxResultBytes ?? DEFAULT_MAX_RESULT_BYTES;
  assertSafeInteger(busyTimeoutMs, "busyTimeoutMs", 0, 60 * 60 * 1000);
  assertSafeInteger(maxPayloadBytes, "maxPayloadBytes", 1, 64 * 1024 * 1024);
  assertSafeInteger(maxResultBytes, "maxResultBytes", 1, 64 * 1024 * 1024);
  return { path: options.path, busyTimeoutMs, maxPayloadBytes, maxResultBytes };
}

export function normalizeAddOptions(options: AddOptions | undefined): NormalizedAddOptions {
  const attempts = options?.attempts ?? DEFAULT_MAX_ATTEMPTS;
  const delayMs = options?.delayMs ?? 0;
  const priority = options?.priority ?? DEFAULT_PRIORITY;
  const attemptTimeoutMs = options?.attemptTimeoutMs ?? null;
  const backoff: BackoffConfig = options?.backoff ?? { type: "none" };
  assertSafeInteger(attempts, "attempts", 1, 100_000);
  assertSafeInteger(delayMs, "delayMs", 0, 30 * 24 * 60 * 60 * 1000);
  assertSafeInteger(priority, "priority", MIN_PRIORITY, MAX_PRIORITY);
  if (attemptTimeoutMs !== null) {
    assertSafeInteger(attemptTimeoutMs, "attemptTimeoutMs", 1, 24 * 60 * 60 * 1000);
  }
  assertBackoff(backoff);
  let dedupeKey: string | null = null;
  if (options?.dedupeKey !== undefined) {
    if (typeof options.dedupeKey !== "string" || options.dedupeKey.length === 0) {
      throw new DurableQValidationError("dedupeKey must be a non-empty string when provided");
    }
    if (options.dedupeKey.length > MAX_DEDUPE_KEY_LENGTH || options.dedupeKey.includes("\0")) {
      throw new DurableQValidationError(
        `dedupeKey must not contain NUL and must be at most ${MAX_DEDUPE_KEY_LENGTH} characters`,
      );
    }
    dedupeKey = options.dedupeKey;
  }
  return { attempts, delayMs, priority, dedupeKey, attemptTimeoutMs, backoff };
}

export function assertLeaseTiming(opts: {
  leaseDurationMs: number;
  heartbeatIntervalMs: number;
  busyTimeoutMs: number;
}): void {
  const { leaseDurationMs: L, heartbeatIntervalMs: H, busyTimeoutMs: B } = opts;
  assertSafeInteger(H, "heartbeatIntervalMs", 1, 24 * 60 * 60 * 1000);
  assertSafeInteger(B, "busyTimeoutMs", 0, 60 * 60 * 1000);
  assertSafeInteger(L, "leaseDurationMs", 1, 24 * 60 * 60 * 1000);
  if (!(L > H + B)) {
    throw new DurableQValidationError(
      `leaseDurationMs (${L}) must be > heartbeatIntervalMs (${H}) + busyTimeoutMs (${B})`,
    );
  }
}

export function normalizeWorkerOptions(
  options: WorkerOptions | undefined,
  busyTimeoutMs: number,
): NormalizedWorkerOptions {
  const concurrency = options?.concurrency ?? DEFAULT_CONCURRENCY;
  const pollIntervalMs = options?.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
  const leaseDurationMs = options?.leaseDurationMs ?? DEFAULT_LEASE_DURATION_MS;
  const heartbeatIntervalMs = options?.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
  const defaultAttemptTimeoutMs = options?.defaultAttemptTimeoutMs ?? DEFAULT_ATTEMPT_TIMEOUT_MS;
  assertSafeInteger(concurrency, "concurrency", MIN_CONCURRENCY, MAX_CONCURRENCY);
  assertSafeInteger(pollIntervalMs, "pollIntervalMs", 1, 60_000);
  assertSafeInteger(defaultAttemptTimeoutMs, "defaultAttemptTimeoutMs", 1, 24 * 60 * 60 * 1000);
  assertLeaseTiming({ leaseDurationMs, heartbeatIntervalMs, busyTimeoutMs });
  return {
    concurrency,
    pollIntervalMs,
    leaseDurationMs,
    heartbeatIntervalMs,
    defaultAttemptTimeoutMs,
  };
}

export function assertHandlerCount(count: number): void {
  if (count < 1) throw new DurableQValidationError("Worker requires at least one handler");
  if (count > MAX_HANDLERS_PER_WORKER) {
    throw new DurableQValidationError(
      `Worker supports at most ${MAX_HANDLERS_PER_WORKER} handlers`,
    );
  }
}

export function assertJobNameList(names: string[]): void {
  assertHandlerCount(names.length);
  const unique = new Set<string>();
  for (const n of names) {
    assertName(n, "job name");
    if (unique.has(n)) throw new DurableQValidationError(`duplicate handler for ${n}`);
    unique.add(n);
  }
}

import type { DatabaseSync } from "node:sqlite";
import { RECOVERY_BATCH_SIZE } from "../constants.ts";
import { DurableQStaleLeaseError, DurableQValidationError } from "../errors.ts";
import { serializeStoredError, storedErrorToJson } from "../codec/error.ts";
import { newJobId } from "../ids.ts";
import { sqliteRowToJobRow } from "../job.ts";
import { delayAfterFailedAttempt } from "../retry/backoff.ts";
import type { BackoffConfig, CancelResult, JobStatus, StoredError } from "../types.ts";
import { backoffFromColumns, backoffToColumns, type JobRow } from "./protocol.ts";

const JOB_COLS = `
  id, queue, name, payload_json, status, priority, available_at, attempt_count, max_attempts,
  backoff_type, backoff_delay_ms, backoff_max_delay_ms, attempt_timeout_ms,
  lease_owner, lease_generation, lease_expires_at, dedupe_key, result_json, error_json,
  created_at, updated_at, started_at, completed_at, failed_at, cancelled_at
`;

export function getJob(db: DatabaseSync, id: string): JobRow | null {
  const raw = db.prepare(`SELECT ${JOB_COLS} FROM durableq_jobs WHERE id = ?`).get(id) as
    Record<string, unknown> | undefined;
  return raw ? sqliteRowToJobRow(raw) : null;
}

export function countJobs(db: DatabaseSync, queue: string, status: JobStatus | undefined): number {
  if (status) {
    const row = db
      .prepare("SELECT COUNT(*) AS c FROM durableq_jobs WHERE queue = ? AND status = ?")
      .get(queue, status) as { c: number };
    return Number(row.c);
  }
  const row = db.prepare("SELECT COUNT(*) AS c FROM durableq_jobs WHERE queue = ?").get(queue) as {
    c: number;
  };
  return Number(row.c);
}

export function enqueueJob(
  db: DatabaseSync,
  args: {
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
  },
): { row: JobRow; deduplicated: boolean } {
  if (args.dedupeKey !== null) {
    const existing = db
      .prepare(
        `SELECT ${JOB_COLS} FROM durableq_jobs WHERE queue = ? AND name = ? AND dedupe_key = ?`,
      )
      .get(args.queue, args.name, args.dedupeKey) as Record<string, unknown> | undefined;
    if (existing) {
      return { row: sqliteRowToJobRow(existing), deduplicated: true };
    }
  }

  const id = newJobId();
  const availableAt = args.nowMs + args.delayMs;
  const cols = backoffToColumns(args.backoff);
  db.prepare(
    `INSERT INTO durableq_jobs (
      id, queue, name, payload_json, status, priority, available_at,
      attempt_count, max_attempts, backoff_type, backoff_delay_ms, backoff_max_delay_ms,
      attempt_timeout_ms, lease_owner, lease_generation, lease_expires_at, dedupe_key,
      result_json, error_json, created_at, updated_at, started_at, completed_at, failed_at, cancelled_at
    ) VALUES (
      @id, @queue, @name, @payload_json, 'queued', @priority, @available_at,
      0, @max_attempts, @backoff_type, @backoff_delay_ms, @backoff_max_delay_ms,
      @attempt_timeout_ms, NULL, 0, NULL, @dedupe_key,
      NULL, NULL, @now, @now, NULL, NULL, NULL, NULL
    )`,
  ).run({
    id,
    queue: args.queue,
    name: args.name,
    payload_json: args.payloadJson,
    priority: args.priority,
    available_at: availableAt,
    max_attempts: args.maxAttempts,
    backoff_type: cols.backoff_type,
    backoff_delay_ms: cols.backoff_delay_ms,
    backoff_max_delay_ms: cols.backoff_max_delay_ms,
    attempt_timeout_ms: args.attemptTimeoutMs,
    dedupe_key: args.dedupeKey,
    now: args.nowMs,
  });
  const row = getJob(db, id);
  if (!row) throw new Error("enqueue inserted but row missing");
  return { row, deduplicated: false };
}

export function claimJob(
  db: DatabaseSync,
  args: {
    nowMs: number;
    queue: string;
    workerId: string;
    names: string[];
    leaseDurationMs: number;
  },
): JobRow | null {
  recoverExpiredLocked(db, args.nowMs);
  if (args.names.length === 0) return null;
  const placeholders = args.names.map((_, i) => `@n${i}`).join(", ");
  const named: Record<string, string | number> = {
    queue: args.queue,
    now: args.nowMs,
    worker_id: args.workerId,
    lease_expires_at: args.nowMs + args.leaseDurationMs,
  };
  args.names.forEach((n, i) => {
    named[`n${i}`] = n;
  });

  const raw = db
    .prepare(
      `UPDATE durableq_jobs SET
         status = 'active',
         attempt_count = attempt_count + 1,
         lease_generation = lease_generation + 1,
         lease_owner = @worker_id,
         lease_expires_at = @lease_expires_at,
         started_at = COALESCE(started_at, @now),
         updated_at = @now
       WHERE id = (
         SELECT id FROM durableq_jobs
         WHERE queue = @queue
           AND status = 'queued'
           AND available_at <= @now
           AND attempt_count < max_attempts
           AND name IN (${placeholders})
         ORDER BY priority DESC, available_at ASC, created_at ASC, id ASC
         LIMIT 1
       )
       RETURNING ${JOB_COLS}`,
    )
    .get(named) as Record<string, unknown> | undefined;

  if (!raw) return null;
  const row = sqliteRowToJobRow(raw);
  db.prepare(
    `INSERT INTO durableq_attempts (job_id, attempt, lease_generation, worker_id, started_at)
     VALUES (?, ?, ?, ?, ?)`,
  ).run(row.id, row.attempt_count, row.lease_generation, args.workerId, args.nowMs);
  return row;
}

export function heartbeatJob(
  db: DatabaseSync,
  args: {
    nowMs: number;
    id: string;
    workerId: string;
    generation: number;
    leaseDurationMs: number;
  },
): boolean {
  const result = db
    .prepare(
      `UPDATE durableq_jobs
       SET lease_expires_at = @next_expiry, updated_at = @now
       WHERE id = @id
         AND status = 'active'
         AND lease_owner = @worker_id
         AND lease_generation = @generation
         AND lease_expires_at > @now`,
    )
    .run({
      next_expiry: args.nowMs + args.leaseDurationMs,
      now: args.nowMs,
      id: args.id,
      worker_id: args.workerId,
      generation: args.generation,
    });
  return Number(result.changes) === 1;
}

export function settleSuccess(
  db: DatabaseSync,
  args: { nowMs: number; id: string; workerId: string; generation: number; resultJson: string },
): JobRow {
  const raw = db
    .prepare(
      `UPDATE durableq_jobs SET
         status = 'completed',
         result_json = @result_json,
         lease_owner = NULL,
         lease_expires_at = NULL,
         completed_at = @now,
         updated_at = @now
       WHERE id = @id
         AND status = 'active'
         AND lease_owner = @worker_id
         AND lease_generation = @generation
         AND lease_expires_at > @now
       RETURNING ${JOB_COLS}`,
    )
    .get({
      result_json: args.resultJson,
      now: args.nowMs,
      id: args.id,
      worker_id: args.workerId,
      generation: args.generation,
    }) as Record<string, unknown> | undefined;
  if (!raw) {
    throw new DurableQStaleLeaseError("stale or expired lease; completion rejected");
  }
  db.prepare(
    `UPDATE durableq_attempts
     SET finished_at = ?, outcome = 'completed'
     WHERE job_id = ? AND lease_generation = ?`,
  ).run(args.nowMs, args.id, args.generation);
  return sqliteRowToJobRow(raw);
}

export function settleFailure(
  db: DatabaseSync,
  args: {
    nowMs: number;
    id: string;
    workerId: string;
    generation: number;
    error: StoredError;
    nonRetryable: boolean;
  },
): JobRow {
  const current = db
    .prepare(
      `SELECT ${JOB_COLS} FROM durableq_jobs
       WHERE id = ? AND status = 'active' AND lease_owner = ? AND lease_generation = ? AND lease_expires_at > ?`,
    )
    .get(args.id, args.workerId, args.generation, args.nowMs) as
    Record<string, unknown> | undefined;
  if (!current) {
    throw new DurableQStaleLeaseError("stale or expired lease; failure settlement rejected");
  }
  const row = sqliteRowToJobRow(current);
  const errorJson = storedErrorToJson(args.error);
  const attemptsRemain = !args.nonRetryable && row.attempt_count < row.max_attempts;
  const backoff = backoffFromColumns(
    row.backoff_type,
    row.backoff_delay_ms,
    row.backoff_max_delay_ms,
  );
  const delay = attemptsRemain ? delayAfterFailedAttempt(backoff, row.attempt_count) : 0;
  const outcome = args.nonRetryable
    ? "non_retryable"
    : attemptsRemain
      ? "failed_retrying"
      : "failed_terminal";

  const params = {
    error_json: errorJson,
    now: args.nowMs,
    id: args.id,
    worker_id: args.workerId,
    generation: args.generation,
    available_at: args.nowMs + delay,
  };
  const raw = attemptsRemain
    ? db
        .prepare(
          `UPDATE durableq_jobs SET
             status = 'queued',
             lease_owner = NULL,
             lease_expires_at = NULL,
             available_at = @available_at,
             error_json = @error_json,
             updated_at = @now
           WHERE id = @id
             AND status = 'active'
             AND lease_owner = @worker_id
             AND lease_generation = @generation
             AND lease_expires_at > @now
           RETURNING ${JOB_COLS}`,
        )
        .get(params)
    : db
        .prepare(
          `UPDATE durableq_jobs SET
             status = 'failed',
             lease_owner = NULL,
             lease_expires_at = NULL,
             error_json = @error_json,
             failed_at = @now,
             updated_at = @now
           WHERE id = @id
             AND status = 'active'
             AND lease_owner = @worker_id
             AND lease_generation = @generation
             AND lease_expires_at > @now
           RETURNING ${JOB_COLS}`,
        )
        .get({
          error_json: errorJson,
          now: args.nowMs,
          id: args.id,
          worker_id: args.workerId,
          generation: args.generation,
        });
  if (!raw) {
    throw new DurableQStaleLeaseError("stale or expired lease; failure settlement rejected");
  }
  db.prepare(
    `UPDATE durableq_attempts
     SET finished_at = ?, outcome = ?, error_json = ?
     WHERE job_id = ? AND lease_generation = ?`,
  ).run(args.nowMs, outcome, errorJson, args.id, args.generation);
  return sqliteRowToJobRow(raw);
}

export function cancelQueued(
  db: DatabaseSync,
  args: { nowMs: number; id: string; queue: string },
): CancelResult {
  const raw = db
    .prepare(
      `UPDATE durableq_jobs SET
         status = 'cancelled',
         cancelled_at = @now,
         updated_at = @now
       WHERE id = @id AND queue = @queue AND status = 'queued'
       RETURNING ${JOB_COLS}`,
    )
    .get({ now: args.nowMs, id: args.id, queue: args.queue }) as
    Record<string, unknown> | undefined;
  if (raw) return { cancelled: true };
  const existing = getJob(db, args.id);
  if (!existing || existing.queue !== args.queue) return { cancelled: false, reason: "missing" };
  if (existing.status === "active") return { cancelled: false, reason: "active" };
  if (existing.status === "completed") return { cancelled: false, reason: "completed" };
  if (existing.status === "failed") return { cancelled: false, reason: "failed" };
  return { cancelled: false, reason: "cancelled" };
}

export function manualRetry(
  db: DatabaseSync,
  args: { nowMs: number; id: string; queue: string },
): JobRow | { reason: string } {
  const existing = db
    .prepare(`SELECT ${JOB_COLS} FROM durableq_jobs WHERE id = ? AND queue = ?`)
    .get(args.id, args.queue) as Record<string, unknown> | undefined;
  if (!existing) return { reason: "missing" };
  const row = sqliteRowToJobRow(existing);
  if (row.status === "active") return { reason: "active" };
  if (row.status === "queued") return { reason: "queued" };
  if (row.status === "completed") return { reason: "completed" };
  if (row.status === "cancelled") return { reason: "cancelled" };
  if (row.status !== "failed") return { reason: "missing" };

  const nextMax = row.attempt_count >= row.max_attempts ? row.attempt_count + 1 : row.max_attempts;
  const raw = db
    .prepare(
      `UPDATE durableq_jobs SET
         status = 'queued',
         available_at = @now,
         max_attempts = @max_attempts,
         failed_at = NULL,
         updated_at = @now
       WHERE id = @id AND status = 'failed'
       RETURNING ${JOB_COLS}`,
    )
    .get({ now: args.nowMs, max_attempts: nextMax, id: args.id }) as
    Record<string, unknown> | undefined;
  if (!raw) return { reason: "missing" };
  return sqliteRowToJobRow(raw);
}

export function cleanJobs(
  db: DatabaseSync,
  args: { queue: string; status: "completed" | "failed" | "cancelled"; before: number },
): number {
  const col =
    args.status === "completed"
      ? "completed_at"
      : args.status === "failed"
        ? "failed_at"
        : "cancelled_at";
  const result = db
    .prepare(
      `DELETE FROM durableq_jobs WHERE queue = ? AND status = ? AND ${col} IS NOT NULL AND ${col} < ?`,
    )
    .run(args.queue, args.status, args.before);
  return Number(result.changes);
}

function recoverOne(db: DatabaseSync, raw: Record<string, unknown>, nowMs: number): void {
  const row = sqliteRowToJobRow(raw);
  const backoff = backoffFromColumns(
    row.backoff_type,
    row.backoff_delay_ms,
    row.backoff_max_delay_ms,
  );
  const attemptsRemain = row.attempt_count < row.max_attempts;
  const err = serializeStoredError(new Error("lease expired"));
  err.name = "LeaseExpiredError";
  const errorJson = storedErrorToJson(err);
  const delay = attemptsRemain ? delayAfterFailedAttempt(backoff, row.attempt_count) : 0;

  const updated = attemptsRemain
    ? db
        .prepare(
          `UPDATE durableq_jobs SET
             status = 'queued',
             lease_owner = NULL,
             lease_expires_at = NULL,
             available_at = @available_at,
             error_json = @error_json,
             updated_at = @now
           WHERE id = @id
             AND status = 'active'
             AND lease_generation = @generation
             AND lease_expires_at <= @now`,
        )
        .run({
          available_at: nowMs + delay,
          error_json: errorJson,
          now: nowMs,
          id: row.id,
          generation: row.lease_generation,
        })
    : db
        .prepare(
          `UPDATE durableq_jobs SET
             status = 'failed',
             lease_owner = NULL,
             lease_expires_at = NULL,
             error_json = @error_json,
             failed_at = @now,
             updated_at = @now
           WHERE id = @id
             AND status = 'active'
             AND lease_generation = @generation
             AND lease_expires_at <= @now`,
        )
        .run({
          error_json: errorJson,
          now: nowMs,
          id: row.id,
          generation: row.lease_generation,
        });
  if (Number(updated.changes) !== 1) return;
  db.prepare(
    `UPDATE durableq_attempts
     SET finished_at = ?, outcome = 'lease_expired', error_json = ?
     WHERE job_id = ? AND lease_generation = ? AND finished_at IS NULL`,
  ).run(nowMs, errorJson, row.id, row.lease_generation);
}

export function recoverExpiredLocked(db: DatabaseSync, nowMs: number): number {
  const rows = db
    .prepare(
      `SELECT ${JOB_COLS} FROM durableq_jobs
       WHERE status = 'active' AND lease_expires_at <= ?
       ORDER BY lease_expires_at ASC
       LIMIT ?`,
    )
    .all(nowMs, RECOVERY_BATCH_SIZE) as Record<string, unknown>[];
  for (const raw of rows) recoverOne(db, raw, nowMs);
  return rows.length;
}

export function nextWakeHint(
  db: DatabaseSync,
  queue: string,
  names: string[],
  nowMs: number,
): number | null {
  if (names.length === 0) return null;
  const placeholders = names.map(() => "?").join(", ");
  const row = db
    .prepare(
      `SELECT MIN(available_at) AS t FROM durableq_jobs
       WHERE queue = ? AND status = 'queued' AND attempt_count < max_attempts AND name IN (${placeholders})`,
    )
    .get(queue, ...names) as { t: number | null } | undefined;
  if (!row || row.t === null) return null;
  const t = Number(row.t);
  return t > nowMs ? t : nowMs;
}

export function requireStatus(status: string): JobStatus {
  if (
    status === "queued" ||
    status === "active" ||
    status === "completed" ||
    status === "failed" ||
    status === "cancelled"
  ) {
    return status;
  }
  throw new DurableQValidationError(`invalid status ${status}`);
}

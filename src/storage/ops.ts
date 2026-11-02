import type { DatabaseSync, StatementSync } from "node:sqlite";
import {
  JOB_STATUSES,
  RECOVERY_BATCH_SIZE,
  RETENTION_BATCH_SIZE,
  RETENTION_INTERVAL_MS,
  RETENTION_MAX_BATCHES,
  SCHEDULE_BATCH_SIZE,
} from "../constants.ts";
import { DurableQStaleLeaseError } from "../errors.ts";
import { serializeStoredError, storedErrorToJson } from "../codec/error.ts";
import { newJobId } from "../ids.ts";
import { sqliteRowToJobRow } from "../job.ts";
import { delayAfterFailedAttempt } from "../retry/backoff.ts";
import type { BackoffConfig, CancelResult, JobStatus, StoredError } from "../types.ts";
import {
  backoffFromColumns,
  backoffToColumns,
  type JobRow,
  type ListPayload,
  type ProgressPayload,
  type RetentionPayload,
  type ScheduleRow,
  type UpsertSchedulePayload,
} from "./protocol.ts";
import { transact } from "./database.ts";
import type { TransactionCuts } from "./session.ts";

// Statements are compiled once per connection; every SQL string used here comes from a bounded set.
const statements = new WeakMap<DatabaseSync, Map<string, StatementSync>>();
function prepare(db: DatabaseSync, sql: string): StatementSync {
  let cache = statements.get(db);
  if (!cache) statements.set(db, (cache = new Map()));
  let stmt = cache.get(sql);
  if (!stmt) cache.set(sql, (stmt = db.prepare(sql)));
  return stmt;
}

const JOB_COLS = `
  id, queue, name, payload_json, status, priority, available_at, attempt_count, max_attempts,
  backoff_type, backoff_delay_ms, backoff_max_delay_ms, attempt_timeout_ms,
  lease_owner, lease_generation, lease_expires_at, dedupe_key, result_json, error_json,
  created_at, updated_at, started_at, completed_at, failed_at, cancelled_at, progress
`;

export function getJob(db: DatabaseSync, id: string): JobRow | null {
  const raw = prepare(db, `SELECT ${JOB_COLS} FROM durableq_jobs WHERE id = ?`).get(id) as
    Record<string, unknown> | undefined;
  return raw ? sqliteRowToJobRow(raw) : null;
}

export function countJobs(db: DatabaseSync, queue: string, status: JobStatus | undefined): number {
  if (status) {
    const row = prepare(
      db,
      "SELECT COUNT(*) AS c FROM durableq_jobs WHERE queue = ? AND status = ?",
    ).get(queue, status) as { c: number };
    return Number(row.c);
  }
  const row = prepare(db, "SELECT COUNT(*) AS c FROM durableq_jobs WHERE queue = ?").get(queue) as {
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
    const existing = prepare(
      db,
      `SELECT ${JOB_COLS} FROM durableq_jobs WHERE queue = ? AND name = ? AND dedupe_key = ?`,
    ).get(args.queue, args.name, args.dedupeKey) as Record<string, unknown> | undefined;
    if (existing) {
      return { row: sqliteRowToJobRow(existing), deduplicated: true };
    }
  }

  const id = newJobId();
  const availableAt = args.nowMs + args.delayMs;
  const cols = backoffToColumns(args.backoff);
  prepare(
    db,
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
  // Pause and schedule materialization share the claim's write transaction, so a claim
  // can never observe a stale pause flag and two processes can never both fire a slot.
  const gate = prepare(
    db,
    `SELECT
         (SELECT paused FROM durableq_queues WHERE queue = @queue) AS paused,
         EXISTS (SELECT 1 FROM durableq_schedules
                 WHERE queue = @queue AND next_run_at <= @now) AS due`,
  ).get({ queue: args.queue, now: args.nowMs }) as { paused: number | null; due: number };
  if (Number(gate.paused) === 1) return null;
  if (Number(gate.due) === 1) materializeSchedules(db, args.queue, args.nowMs);
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

  const raw = prepare(
    db,
    `UPDATE durableq_jobs SET
         status = 'active',
         attempt_count = attempt_count + 1,
         lease_generation = lease_generation + 1,
         lease_owner = @worker_id,
         lease_expires_at = @lease_expires_at,
         progress = 0,
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
  ).get(named) as Record<string, unknown> | undefined;

  if (!raw) return null;
  const row = sqliteRowToJobRow(raw);
  prepare(
    db,
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
  const result = prepare(
    db,
    `UPDATE durableq_jobs
       SET lease_expires_at = @next_expiry, updated_at = @now
       WHERE id = @id
         AND status = 'active'
         AND lease_owner = @worker_id
         AND lease_generation = @generation
         AND lease_expires_at > @now`,
  ).run({
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
  const raw = prepare(
    db,
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
  ).get({
    result_json: args.resultJson,
    now: args.nowMs,
    id: args.id,
    worker_id: args.workerId,
    generation: args.generation,
  }) as Record<string, unknown> | undefined;
  if (!raw) {
    throw new DurableQStaleLeaseError("stale or expired lease; completion rejected");
  }
  prepare(
    db,
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
  const current = prepare(
    db,
    `SELECT ${JOB_COLS} FROM durableq_jobs
       WHERE id = ? AND status = 'active' AND lease_owner = ? AND lease_generation = ? AND lease_expires_at > ?`,
  ).get(args.id, args.workerId, args.generation, args.nowMs) as Record<string, unknown> | undefined;
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
    ? prepare(
        db,
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
      ).get(params)
    : prepare(
        db,
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
      ).get({
        error_json: errorJson,
        now: args.nowMs,
        id: args.id,
        worker_id: args.workerId,
        generation: args.generation,
      });
  if (!raw) {
    throw new DurableQStaleLeaseError("stale or expired lease; failure settlement rejected");
  }
  prepare(
    db,
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
  const raw = prepare(
    db,
    `UPDATE durableq_jobs SET
         status = 'cancelled',
         cancelled_at = @now,
         updated_at = @now
       WHERE id = @id AND queue = @queue AND status = 'queued'
       RETURNING ${JOB_COLS}`,
  ).get({ now: args.nowMs, id: args.id, queue: args.queue }) as Record<string, unknown> | undefined;
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
  const existing = prepare(
    db,
    `SELECT ${JOB_COLS} FROM durableq_jobs WHERE id = ? AND queue = ?`,
  ).get(args.id, args.queue) as Record<string, unknown> | undefined;
  if (!existing) return { reason: "missing" };
  const row = sqliteRowToJobRow(existing);
  if (row.status === "active") return { reason: "active" };
  if (row.status === "queued") return { reason: "queued" };
  if (row.status === "completed") return { reason: "completed" };
  if (row.status === "cancelled") return { reason: "cancelled" };
  if (row.status !== "failed") return { reason: "missing" };

  const nextMax = row.attempt_count >= row.max_attempts ? row.attempt_count + 1 : row.max_attempts;
  const raw = prepare(
    db,
    `UPDATE durableq_jobs SET
         status = 'queued',
         available_at = @now,
         max_attempts = @max_attempts,
         failed_at = NULL,
         error_json = NULL,
         updated_at = @now
       WHERE id = @id AND status = 'failed'
       RETURNING ${JOB_COLS}`,
  ).get({ now: args.nowMs, max_attempts: nextMax, id: args.id }) as
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
  const result = prepare(
    db,
    `DELETE FROM durableq_jobs WHERE queue = ? AND status = ? AND ${col} IS NOT NULL AND ${col} < ?`,
  ).run(args.queue, args.status, args.before);
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
    ? prepare(
        db,
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
      ).run({
        available_at: nowMs + delay,
        error_json: errorJson,
        now: nowMs,
        id: row.id,
        generation: row.lease_generation,
      })
    : prepare(
        db,
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
      ).run({
        error_json: errorJson,
        now: nowMs,
        id: row.id,
        generation: row.lease_generation,
      });
  if (Number(updated.changes) !== 1) return;
  prepare(
    db,
    `UPDATE durableq_attempts
     SET finished_at = ?, outcome = 'lease_expired', error_json = ?
     WHERE job_id = ? AND lease_generation = ? AND finished_at IS NULL`,
  ).run(nowMs, errorJson, row.id, row.lease_generation);
}

export function recoverExpiredLocked(db: DatabaseSync, nowMs: number): number {
  const rows = prepare(
    db,
    `SELECT ${JOB_COLS} FROM durableq_jobs
       WHERE status = 'active' AND lease_expires_at <= ?
       ORDER BY lease_expires_at ASC
       LIMIT ?`,
  ).all(nowMs, RECOVERY_BATCH_SIZE) as Record<string, unknown>[];
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
  const row = prepare(
    db,
    `SELECT MIN(available_at) AS t FROM durableq_jobs
       WHERE queue = ? AND status = 'queued' AND attempt_count < max_attempts AND name IN (${placeholders})`,
  ).get(queue, ...names) as { t: number | null } | undefined;
  if (!row || row.t === null) return null;
  const t = Number(row.t);
  return t > nowMs ? t : nowMs;
}

export function isPaused(db: DatabaseSync, queue: string): boolean {
  const row = prepare(db, "SELECT paused FROM durableq_queues WHERE queue = ?").get(queue) as
    { paused: number } | undefined;
  return Number(row?.paused ?? 0) === 1;
}

export function setPaused(
  db: DatabaseSync,
  args: { nowMs: number; queue: string; paused: boolean },
): void {
  prepare(
    db,
    `INSERT INTO durableq_queues (queue, paused, updated_at) VALUES (?, ?, ?)
     ON CONFLICT (queue) DO UPDATE SET paused = excluded.paused, updated_at = excluded.updated_at`,
  ).run(args.queue, args.paused ? 1 : 0, args.nowMs);
}

export function updateProgress(db: DatabaseSync, args: ProgressPayload): void {
  const result = prepare(
    db,
    `UPDATE durableq_jobs SET progress = @progress, updated_at = @now
       WHERE id = @id
         AND status = 'active'
         AND lease_owner = @worker_id
         AND lease_generation = @generation
         AND lease_expires_at > @now`,
  ).run({
    progress: args.progress,
    now: args.nowMs,
    id: args.id,
    worker_id: args.workerId,
    generation: args.generation,
  });
  if (Number(result.changes) !== 1) {
    throw new DurableQStaleLeaseError("stale or expired lease; progress update rejected");
  }
}

export type QueueStatsRow = {
  queued: number;
  active: number;
  completed: number;
  failed: number;
  cancelled: number;
  due: number;
  delayed: number;
  retrying: number;
  oldestQueuedAgeMs: number | null;
  paused: boolean;
};

export function queueStats(db: DatabaseSync, queue: string, nowMs: number): QueueStatsRow {
  const out: QueueStatsRow = {
    queued: 0,
    active: 0,
    completed: 0,
    failed: 0,
    cancelled: 0,
    due: 0,
    delayed: 0,
    retrying: 0,
    oldestQueuedAgeMs: null,
    paused: isPaused(db, queue),
  };
  const counts = prepare(
    db,
    "SELECT status, COUNT(*) AS c FROM durableq_jobs WHERE queue = ? GROUP BY status",
  ).all(queue) as { status: JobStatus; c: number }[];
  for (const r of counts) out[r.status] = Number(r.c);
  const delayed = prepare(
    db,
    `SELECT COUNT(*) AS c FROM durableq_jobs
       WHERE queue = ? AND status = 'queued' AND available_at > ?`,
  ).get(queue, nowMs) as { c: number };
  out.delayed = Number(delayed.c);
  out.due = out.queued - out.delayed;
  const oldest = prepare(
    db,
    `SELECT MIN(available_at) AS t FROM durableq_jobs
       WHERE queue = ? AND status = 'queued' AND available_at <= ?`,
  ).get(queue, nowMs) as { t: number | null };
  out.oldestQueuedAgeMs = oldest.t === null ? null : nowMs - Number(oldest.t);
  const retrying = prepare(
    db,
    `SELECT COUNT(*) AS c FROM durableq_jobs
       WHERE queue = ? AND status = 'queued' AND attempt_count > 0`,
  ).get(queue) as { c: number };
  out.retrying = Number(retrying.c);
  return out;
}

export function listJobs(
  db: DatabaseSync,
  args: ListPayload,
): { rows: JobRow[]; next: { updatedAt: number; rowid: number } | null } {
  const statuses = args.status ? [args.status] : JOB_STATUSES;
  const where = [
    "queue = @queue",
    "status = @status",
    ...(args.name !== undefined ? ["name = @name"] : []),
    ...(args.cursor ? ["(updated_at, rowid) < (@cursor_updated, @cursor_rowid)"] : []),
  ].join(" AND ");
  const stmt = prepare(
    db,
    `SELECT rowid AS _rowid, ${JOB_COLS} FROM durableq_jobs
     WHERE ${where}
     ORDER BY updated_at DESC, rowid DESC
     LIMIT @limit`,
  );
  // One index range per status, merged here: each leg is O(limit), never a full scan.
  const merged: { rowid: number; raw: Record<string, unknown> }[] = [];
  for (const status of statuses) {
    const params: Record<string, string | number> = {
      queue: args.queue,
      status,
      limit: args.limit + 1,
    };
    if (args.name !== undefined) params.name = args.name;
    if (args.cursor) {
      params.cursor_updated = args.cursor.updatedAt;
      params.cursor_rowid = args.cursor.rowid;
    }
    for (const raw of stmt.all(params) as Record<string, unknown>[]) {
      merged.push({ rowid: Number(raw._rowid), raw });
    }
  }
  merged.sort((a, b) => Number(b.raw.updated_at) - Number(a.raw.updated_at) || b.rowid - a.rowid);
  const page = merged.slice(0, args.limit);
  const last = page.at(-1);
  const next =
    merged.length > args.limit && last
      ? { updatedAt: Number(last.raw.updated_at), rowid: last.rowid }
      : null;
  return { rows: page.map((m) => sqliteRowToJobRow(m.raw)), next };
}

function sqliteRowToScheduleRow(raw: Record<string, unknown>): ScheduleRow {
  const nullable = (v: unknown) => (v === null || v === undefined ? null : Number(v));
  return {
    queue: String(raw.queue),
    id: String(raw.id),
    name: String(raw.name),
    payload_json: String(raw.payload_json),
    every_ms: Number(raw.every_ms),
    next_run_at: Number(raw.next_run_at),
    max_attempts: Number(raw.max_attempts),
    priority: Number(raw.priority),
    attempt_timeout_ms: nullable(raw.attempt_timeout_ms),
    backoff_type: raw.backoff_type as ScheduleRow["backoff_type"],
    backoff_delay_ms: Number(raw.backoff_delay_ms),
    backoff_max_delay_ms: nullable(raw.backoff_max_delay_ms),
    last_job_id: raw.last_job_id === null ? null : String(raw.last_job_id),
    created_at: Number(raw.created_at),
    updated_at: Number(raw.updated_at),
  };
}

export function getSchedule(db: DatabaseSync, queue: string, id: string): ScheduleRow | null {
  const raw = prepare(db, "SELECT * FROM durableq_schedules WHERE queue = ? AND id = ?").get(
    queue,
    id,
  ) as Record<string, unknown> | undefined;
  return raw ? sqliteRowToScheduleRow(raw) : null;
}

export function listSchedules(db: DatabaseSync, queue: string): ScheduleRow[] {
  const rows = prepare(db, "SELECT * FROM durableq_schedules WHERE queue = ? ORDER BY id").all(
    queue,
  ) as Record<string, unknown>[];
  return rows.map(sqliteRowToScheduleRow);
}

export function removeSchedule(db: DatabaseSync, queue: string, id: string): boolean {
  const result = prepare(db, "DELETE FROM durableq_schedules WHERE queue = ? AND id = ?").run(
    queue,
    id,
  );
  return Number(result.changes) === 1;
}

export function upsertSchedule(
  db: DatabaseSync,
  args: UpsertSchedulePayload,
): { row: ScheduleRow; outcome: "created" | "updated" | "unchanged" } {
  const cols = backoffToColumns(args.backoff);
  const existing = getSchedule(db, args.queue, args.id);
  const params = {
    queue: args.queue,
    id: args.id,
    name: args.name,
    payload_json: args.payloadJson,
    every_ms: args.everyMs,
    max_attempts: args.maxAttempts,
    priority: args.priority,
    attempt_timeout_ms: args.attemptTimeoutMs,
    ...cols,
    now: args.nowMs,
  };
  let outcome: "created" | "updated" | "unchanged";
  if (!existing) {
    prepare(
      db,
      `INSERT INTO durableq_schedules (
         queue, id, name, payload_json, every_ms, next_run_at, max_attempts, priority,
         attempt_timeout_ms, backoff_type, backoff_delay_ms, backoff_max_delay_ms,
         last_job_id, created_at, updated_at
       ) VALUES (
         @queue, @id, @name, @payload_json, @every_ms, @next_run_at, @max_attempts, @priority,
         @attempt_timeout_ms, @backoff_type, @backoff_delay_ms, @backoff_max_delay_ms,
         NULL, @now, @now
       )`,
    ).run({ ...params, next_run_at: args.nowMs + args.everyMs });
    outcome = "created";
  } else if (
    existing.name === params.name &&
    existing.payload_json === params.payload_json &&
    existing.every_ms === params.every_ms &&
    existing.max_attempts === params.max_attempts &&
    existing.priority === params.priority &&
    existing.attempt_timeout_ms === params.attempt_timeout_ms &&
    existing.backoff_type === params.backoff_type &&
    existing.backoff_delay_ms === params.backoff_delay_ms &&
    existing.backoff_max_delay_ms === params.backoff_max_delay_ms
  ) {
    return { row: existing, outcome: "unchanged" };
  } else {
    // A new interval restarts the grid; other changes keep the next occurrence time.
    const nextRunAt =
      existing.every_ms === args.everyMs ? existing.next_run_at : args.nowMs + args.everyMs;
    prepare(
      db,
      `UPDATE durableq_schedules SET
         name = @name, payload_json = @payload_json, every_ms = @every_ms,
         next_run_at = @next_run_at, max_attempts = @max_attempts, priority = @priority,
         attempt_timeout_ms = @attempt_timeout_ms, backoff_type = @backoff_type,
         backoff_delay_ms = @backoff_delay_ms, backoff_max_delay_ms = @backoff_max_delay_ms,
         updated_at = @now
       WHERE queue = @queue AND id = @id`,
    ).run({ ...params, next_run_at: nextRunAt });
    outcome = "updated";
  }
  const row = getSchedule(db, args.queue, args.id);
  if (!row) throw new Error("schedule upserted but row missing");
  return { row, outcome };
}

export function scheduleOccurrenceKey(scheduleId: string, slotMs: number): string {
  return `durableq:schedule:${scheduleId}:${slotMs}`;
}

/**
 * Fixed-rate schedules with coalesced catch-up: each due schedule yields at most one job per
 * call (skipped while its previous occurrence is still queued or active), identified by the
 * latest due grid slot; next_run_at moves to the following slot. Must run inside the caller's write transaction.
 */
export function materializeSchedules(db: DatabaseSync, queue: string, nowMs: number): void {
  const due = prepare(
    db,
    `SELECT * FROM durableq_schedules WHERE queue = ? AND next_run_at <= ?
       ORDER BY next_run_at LIMIT ?`,
  ).all(queue, nowMs, SCHEDULE_BATCH_SIZE) as Record<string, unknown>[];
  const outstandingStmt = prepare(
    db,
    "SELECT 1 AS x FROM durableq_jobs WHERE id = ? AND status IN ('queued', 'active')",
  );
  const advance = prepare(
    db,
    `UPDATE durableq_schedules SET next_run_at = ?, last_job_id = ?, updated_at = ?
     WHERE queue = ? AND id = ?`,
  );
  for (const raw of due) {
    const s = sqliteRowToScheduleRow(raw);
    // The occurrence is the latest grid slot <= now; missed earlier slots are coalesced into it.
    const slot = s.next_run_at + s.every_ms * Math.floor((nowMs - s.next_run_at) / s.every_ms);
    let lastJobId = s.last_job_id;
    const outstanding = lastJobId !== null && outstandingStmt.get(lastJobId) !== undefined;
    if (!outstanding) {
      const { row } = enqueueJob(db, {
        nowMs,
        queue,
        name: s.name,
        payloadJson: s.payload_json,
        maxAttempts: s.max_attempts,
        delayMs: 0,
        priority: s.priority,
        dedupeKey: scheduleOccurrenceKey(s.id, slot),
        attemptTimeoutMs: s.attempt_timeout_ms,
        backoff: backoffFromColumns(s.backoff_type, s.backoff_delay_ms, s.backoff_max_delay_ms),
      });
      lastJobId = row.id;
    }
    advance.run(slot + s.every_ms, lastJobId, nowMs, queue, s.id);
  }
}

/**
 * Deletes expired terminal jobs in bounded transactions. A per-queue slot in durableq_queues
 * makes at most one process run retention per RETENTION_INTERVAL_MS.
 */
export function applyRetention(
  db: DatabaseSync,
  args: RetentionPayload,
  cuts?: TransactionCuts,
): number {
  if (args.rules.length === 0) return 0;
  const acquired = transact(db, () => {
    prepare(
      db,
      `INSERT INTO durableq_queues (queue, updated_at) VALUES (?, ?)
       ON CONFLICT (queue) DO NOTHING`,
    ).run(args.queue, args.nowMs);
    const res = prepare(
      db,
      `UPDATE durableq_queues SET retention_next_at = ?
         WHERE queue = ? AND retention_next_at <= ?`,
    ).run(args.nowMs + RETENTION_INTERVAL_MS, args.queue, args.nowMs);
    return Number(res.changes) === 1;
  });
  if (!acquired) return 0;
  let deleted = 0;
  for (const rule of args.rules) {
    const col =
      rule.status === "completed"
        ? "completed_at"
        : rule.status === "failed"
          ? "failed_at"
          : "cancelled_at";
    // updated_at equals the terminal timestamp for terminal rows; it drives the index range.
    const stmt = prepare(
      db,
      `DELETE FROM durableq_jobs WHERE rowid IN (
         SELECT rowid FROM durableq_jobs
         WHERE queue = ? AND status = ? AND updated_at < ? AND ${col} IS NOT NULL AND ${col} < ?
         ORDER BY updated_at
         LIMIT ?
       )`,
    );
    const cutoff = args.nowMs - rule.ageMs;
    for (let i = 0; i < RETENTION_MAX_BATCHES; i++) {
      cuts?.beforeBegin?.();
      const n = transact(db, () => {
        cuts?.afterBegin?.();
        const res = stmt.run(args.queue, rule.status, cutoff, cutoff, RETENTION_BATCH_SIZE);
        cuts?.beforeCommit?.();
        return Number(res.changes);
      });
      cuts?.afterCommit?.();
      deleted += n;
      if (n < RETENTION_BATCH_SIZE) break;
    }
  }
  return deleted;
}

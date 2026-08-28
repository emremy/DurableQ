import { DatabaseSync } from "node:sqlite";
import assert from "node:assert/strict";
import { JOB_STATUSES } from "../../src/constants.ts";
import { decodeJson } from "../../src/codec/json.ts";

export type JobInvariantRow = {
  id: string;
  queue: string;
  name: string;
  payload_json: string;
  status: string;
  priority: number;
  available_at: number;
  attempt_count: number;
  max_attempts: number;
  backoff_type: string;
  backoff_delay_ms: number;
  backoff_max_delay_ms: number | null;
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
};

export function assertDatabaseInvariants(dbPath: string, nowMs: number = Date.now()): void {
  const db = new DatabaseSync(dbPath, { timeout: 5000, defensive: true, allowExtension: false });
  try {
    const quick = db.prepare("PRAGMA quick_check").get() as { quick_check: string };
    assert.equal(quick.quick_check, "ok", `quick_check: ${JSON.stringify(quick)}`);

    const fk = db.prepare("PRAGMA foreign_key_check").all() as unknown[];
    assert.equal(fk.length, 0, `foreign_key_check: ${JSON.stringify(fk)}`);

    const jobs = db.prepare("SELECT * FROM durableq_jobs").all() as JobInvariantRow[];
    const attempts = db
      .prepare("SELECT job_id, attempt, lease_generation, outcome FROM durableq_attempts")
      .all() as {
      job_id: string;
      attempt: number;
      lease_generation: number;
      outcome: string | null;
    }[];

    const attemptKey = new Set<string>();
    for (const a of attempts) {
      const k = `${a.job_id}:${a.lease_generation}`;
      assert.equal(attemptKey.has(k), false, `duplicate attempt ${k}`);
      attemptKey.add(k);
    }

    const byJobAttempts = new Map<string, number>();
    for (const a of attempts) {
      byJobAttempts.set(a.job_id, (byJobAttempts.get(a.job_id) ?? 0) + 1);
    }

    const dedupeSeen = new Set<string>();
    const activeIds = new Set<string>();

    for (const job of jobs) {
      const ctx = `job ${job.id}`;
      assert.ok(
        JOB_STATUSES.includes(job.status as (typeof JOB_STATUSES)[number]),
        `${ctx} bad status`,
      );
      assert.ok(job.attempt_count >= 0, `${ctx} attempt_count < 0`);
      assert.ok(job.attempt_count <= job.max_attempts, `${ctx} attempt_count > max_attempts`);
      assert.ok(job.lease_generation >= 0, `${ctx} generation`);

      if (job.status === "active") {
        assert.ok(job.lease_owner !== null, `${ctx} active missing owner`);
        assert.ok(job.lease_expires_at !== null, `${ctx} active missing expiry`);
        assert.ok(job.lease_generation >= 1, `${ctx} active generation`);
        activeIds.add(job.id);
      } else {
        assert.equal(job.lease_owner, null, `${ctx} non-active has owner`);
        assert.equal(job.lease_expires_at, null, `${ctx} non-active has expiry`);
      }

      if (job.status === "queued") {
        assert.ok(typeof job.available_at === "number", `${ctx} queued available_at`);
        if (job.available_at > nowMs) {
          assert.notEqual(job.status, "active");
        }
      }

      if (job.status === "completed") {
        assert.ok(job.completed_at !== null, `${ctx} completed_at`);
        assert.equal(job.failed_at, null, `${ctx} completed has failed_at`);
        assert.equal(job.cancelled_at, null, `${ctx} completed has cancelled_at`);
      } else {
        assert.equal(job.completed_at, null, `${ctx} unexpected completed_at`);
      }

      if (job.status === "failed") {
        assert.ok(job.failed_at !== null, `${ctx} failed_at`);
      } else {
        assert.equal(job.failed_at, null, `${ctx} unexpected failed_at`);
      }

      if (job.status === "cancelled") {
        assert.ok(job.cancelled_at !== null, `${ctx} cancelled_at`);
      } else {
        assert.equal(job.cancelled_at, null, `${ctx} unexpected cancelled_at`);
      }

      decodeJson(job.payload_json);
      if (job.result_json !== null) decodeJson(job.result_json);
      if (job.error_json !== null) decodeJson(job.error_json);

      const historyCount = byJobAttempts.get(job.id) ?? 0;
      assert.equal(
        historyCount,
        job.attempt_count,
        `${ctx} attempt history ${historyCount} != ${job.attempt_count}`,
      );

      assert.ok(["none", "fixed", "exponential"].includes(job.backoff_type), `${ctx} backoff`);
      assert.ok(job.backoff_delay_ms >= 0, `${ctx} backoff delay`);

      if (job.dedupe_key !== null) {
        const dk = `${job.queue}\0${job.name}\0${job.dedupe_key}`;
        assert.equal(dedupeSeen.has(dk), false, `${ctx} duplicate dedupe`);
        dedupeSeen.add(dk);
      }
    }

    for (const a of attempts) {
      assert.ok(
        jobs.some((j) => j.id === a.job_id),
        `orphan attempt ${a.job_id}`,
      );
    }
  } finally {
    db.close();
  }
}

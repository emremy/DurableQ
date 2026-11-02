import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

function plan(db: DatabaseSync, sql: string): string {
  const rows = db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all() as { detail: string }[];
  return rows.map((r) => r.detail).join(" | ");
}

test("v0.2.0 inspection, retention and schedule queries are index range scans", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.addMany(Array.from({ length: 200 }, (_, i) => ({ name: `n${i % 5}`, data: { i } })));
    await q.schedule("tick", { id: "s", everyMs: 1_000, data: null });
    await q.close();
    const db = new DatabaseSync(tmp.path);
    try {
      const cases: [string, string, RegExp][] = [
        [
          "claim (must read the first eligible entry, not sort the backlog)",
          `SELECT id FROM durableq_jobs
           WHERE queue = 'app' AND status = 'queued' AND available_at <= 5
             AND attempt_count < max_attempts AND name IN ('n1', 'n2')
           ORDER BY priority DESC, available_at ASC, created_at ASC, id ASC
           LIMIT 1`,
          /SEARCH durableq_jobs USING INDEX durableq_jobs_claim_idx \(queue=\? AND status=\?\)/,
        ],
        [
          "list by status",
          `SELECT rowid, * FROM durableq_jobs WHERE queue = 'app' AND status = 'failed'
           AND (updated_at, rowid) < (1, 1) ORDER BY updated_at DESC, rowid DESC LIMIT 51`,
          /SEARCH durableq_jobs USING INDEX durableq_jobs_status_updated_idx/,
        ],
        [
          "list by name",
          `SELECT rowid, * FROM durableq_jobs WHERE queue = 'app' AND status = 'queued'
           AND name = 'n1' ORDER BY updated_at DESC, rowid DESC LIMIT 51`,
          /SEARCH durableq_jobs USING INDEX durableq_jobs_name_status_updated_idx/,
        ],
        [
          "stats counts",
          "SELECT status, COUNT(*) FROM durableq_jobs WHERE queue = 'app' GROUP BY status",
          /USING COVERING INDEX/,
        ],
        [
          "stats delayed",
          `SELECT COUNT(*) FROM durableq_jobs WHERE queue = 'app' AND status = 'queued'
           AND available_at > 5`,
          /SEARCH durableq_jobs USING COVERING INDEX durableq_jobs_claim_idx/,
        ],
        [
          "retention batch",
          `SELECT rowid FROM durableq_jobs WHERE queue = 'app' AND status = 'completed'
           AND updated_at < 5 AND completed_at IS NOT NULL AND completed_at < 5
           ORDER BY updated_at LIMIT 500`,
          /SEARCH durableq_jobs USING INDEX durableq_jobs_status_updated_idx \(queue=\? AND status=\? AND updated_at<\?\)/,
        ],
        [
          "due schedules",
          `SELECT * FROM durableq_schedules WHERE queue = 'app' AND next_run_at <= 5
           ORDER BY next_run_at LIMIT 50`,
          /SEARCH durableq_schedules USING INDEX durableq_schedules_due_idx/,
        ],
      ];
      for (const [label, sql, expected] of cases) {
        const text = plan(db, sql);
        assert.match(text, expected, `${label}: ${text}`);
        assert.doesNotMatch(text, /USE TEMP B-TREE/, `${label} sorts: ${text}`);
      }
    } finally {
      db.close();
    }
  } finally {
    tmp.cleanup();
  }
});

import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

function planText(db: DatabaseSync, sql: string): string {
  const plan = db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all() as { detail: string }[];
  return plan.map((p) => p.detail).join(" | ");
}

function usesIndex(text: string): boolean {
  return /INDEX/i.test(text) || /SEARCH/i.test(text);
}

test("hot queries use indexes rather than full scans", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    for (let i = 0; i < 40; i++) {
      await q.add("n", { i }, { priority: i, dedupeKey: `k-${i}` });
    }
    await q.close();
    const db = new DatabaseSync(tmp.path);
    try {
      const now = Date.now();
      const claim = planText(
        db,
        `SELECT id FROM durableq_jobs
         WHERE queue = 'app' AND status = 'queued' AND available_at <= ${now}
           AND attempt_count < max_attempts AND name IN ('n')
         ORDER BY priority DESC, available_at ASC, created_at ASC, id ASC
         LIMIT 1`,
      );
      const recovery = planText(
        db,
        `SELECT id FROM durableq_jobs
         WHERE status = 'active' AND lease_expires_at <= ${now}
         ORDER BY lease_expires_at ASC LIMIT 50`,
      );
      const dedupe = planText(
        db,
        `SELECT id FROM durableq_jobs WHERE queue = 'app' AND name = 'n' AND dedupe_key = 'k-1'`,
      );
      const clean = planText(
        db,
        `DELETE FROM durableq_jobs WHERE queue = 'app' AND status = 'completed' AND completed_at IS NOT NULL AND completed_at < ${now}`,
      );
      assert.equal(/SCAN/i.test(claim) && !usesIndex(claim), false, claim);
      assert.ok(usesIndex(recovery) || !/SCAN durableq_jobs/i.test(recovery), recovery);
      assert.ok(usesIndex(dedupe), dedupe);
      assert.ok(typeof clean === "string");
      const journal = db.prepare("PRAGMA journal_mode").get() as { journal_mode: string };
      const sync = db.prepare("PRAGMA synchronous").get() as { synchronous: number };
      assert.equal(String(journal.journal_mode).toLowerCase(), "wal");
      assert.ok(Number(sync.synchronous) >= 2);
    } finally {
      db.close();
    }
  } finally {
    tmp.cleanup();
  }
});

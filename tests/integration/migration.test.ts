import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { SCHEMA_VERSION } from "../../src/constants.ts";
import { DurableQSchemaVersionError } from "../../src/errors.ts";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { createV010Database, readRows } from "../support/v010-db.ts";

function schemaObjects(path: string): string[] {
  return readRows(path, "SELECT type || ':' || name AS o FROM sqlite_master ORDER BY 1").map((r) =>
    String(r.o),
  );
}

function version(path: string): string {
  const [row] = readRows(path, "SELECT value FROM _durableq_meta WHERE key = 'schema_version'");
  return String(row?.value);
}

test("fresh database is created at the current schema version", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.count();
    assert.equal(version(tmp.path), String(SCHEMA_VERSION));
    const objects = schemaObjects(tmp.path);
    for (const o of [
      "table:durableq_queues",
      "table:durableq_schedules",
      "index:durableq_jobs_status_updated_idx",
      "index:durableq_jobs_name_status_updated_idx",
      "index:durableq_schedules_due_idx",
    ]) {
      assert.ok(objects.includes(o), `missing ${o}`);
    }
    const cols = readRows(tmp.path, "PRAGMA table_info(durableq_jobs)").map((c) => c.name);
    assert.ok(cols.includes("progress"));
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("genuine v0.1.0 database upgrades without losing or altering jobs", async () => {
  const tmp = tempDbPath();
  createV010Database(tmp.path);
  const jobsBefore = readRows(tmp.path, "SELECT * FROM durableq_jobs ORDER BY id");
  const attemptsBefore = readRows(
    tmp.path,
    "SELECT * FROM durableq_attempts ORDER BY job_id, lease_generation",
  );
  assert.equal(version(tmp.path), "1");
  assert.equal(jobsBefore.length, 9);

  const q = openQueue(tmp.path);
  try {
    assert.equal(await q.count(), 8); // one job lives in queue "other"
    assert.equal(version(tmp.path), String(SCHEMA_VERSION));

    const jobsAfter = readRows(tmp.path, "SELECT * FROM durableq_jobs ORDER BY id");
    assert.deepEqual(
      jobsAfter.map(({ progress, ...rest }) => {
        assert.equal(progress, 0);
        return rest;
      }),
      jobsBefore,
    );
    assert.deepEqual(
      readRows(tmp.path, "SELECT * FROM durableq_attempts ORDER BY job_id, lease_generation"),
      attemptsBefore,
    );
    assertDatabaseInvariants(tmp.path, 1_700_000_000_000);
    const [claimIdx] = readRows(
      tmp.path,
      "SELECT sql FROM sqlite_master WHERE name = 'durableq_jobs_claim_idx'",
    );
    assert.match(
      String(claimIdx?.sql),
      /\(queue, status, priority DESC, available_at, created_at, id\)/,
      "v0.1.0 claim index must be rebuilt in claim order",
    );

    const byName = new Map(jobsBefore.map((j) => [String(j.name), String(j.id)]));
    const running = await q.get(byName.get("running")!);
    assert.equal(running?.status, "active");
    assert.equal(running?.leaseOwner, "w-old");
    assert.equal(running?.progress, 0);
    const done = await q.get(byName.get("done")!);
    assert.deepEqual(done?.result, { ok: true, n: 1 });
    const dead = await q.get(byName.get("dead")!);
    assert.equal(dead?.error?.message, "boom");

    // Existing dedupe keys keep deduplicating after the upgrade.
    const again = await q.add("queued.now", { k: "dup" }, { dedupeKey: "dk-queued" });
    assert.equal(again.deduplicated, true);
    assert.equal(again.job.id, byName.get("queued.now"));

    const stats = await q.stats();
    assert.equal(stats.queued, 3);
    assert.equal(stats.active, 2);
    assert.equal(stats.completed, 1);
    assert.equal(stats.failed, 1);
    assert.equal(stats.cancelled, 1);
    assert.equal(stats.delayed, 1);
    assert.equal(stats.retrying, 1);
    assert.equal(stats.paused, false);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("upgraded jobs are processed and expired v0.1.0 leases are recovered", async () => {
  const tmp = tempDbPath();
  createV010Database(tmp.path);
  const q = openQueue(tmp.path);
  const seen: string[] = [];
  const worker = new Worker(
    q,
    {
      "queued.now": async (job) => {
        seen.push(job.name);
        return null;
      },
      expired: async (job, ctx) => {
        await ctx.updateProgress(50);
        seen.push(`${job.name}:${ctx.attempt}`);
        return null;
      },
      retrying: async (job) => {
        seen.push(job.name);
        return null;
      },
    },
    { pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 100 },
  );
  try {
    await worker.start();
    await eventually(() => assert.equal(seen.length, 3));
    assert.ok(seen.includes("expired:2"), seen.join(","));
    assert.equal(await q.count({ status: "completed" }), 4);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("reopening a migrated database is a no-op", async () => {
  const tmp = tempDbPath();
  createV010Database(tmp.path);
  try {
    const q1 = openQueue(tmp.path);
    await q1.count();
    await q1.close();
    const before = schemaObjects(tmp.path);
    const q2 = openQueue(tmp.path);
    await q2.count();
    await q2.close();
    assert.deepEqual(schemaObjects(tmp.path), before);
    assert.equal(version(tmp.path), String(SCHEMA_VERSION));
  } finally {
    tmp.cleanup();
  }
});

test("a failed migration rolls back and leaves the v0.1.0 database intact", async () => {
  const tmp = tempDbPath();
  createV010Database(tmp.path);
  const before = schemaObjects(tmp.path);
  process.env.DURABLEQ_TEST_MIGRATE_FAULT = JSON.stringify({
    point: "migrate:before-commit",
    action: "throw",
  });
  const q1 = openQueue(tmp.path);
  try {
    await assert.rejects(() => q1.count(), /injected fault/);
  } finally {
    delete process.env.DURABLEQ_TEST_MIGRATE_FAULT;
    await q1.close().catch(() => undefined);
  }
  try {
    assert.equal(version(tmp.path), "1");
    assert.deepEqual(schemaObjects(tmp.path), before);
    const q2 = openQueue(tmp.path);
    assert.equal(await q2.count(), 8);
    await q2.close();
    assert.equal(version(tmp.path), String(SCHEMA_VERSION));
  } finally {
    tmp.cleanup();
  }
});

test("a database from a newer DurableQ is refused without modification", async () => {
  const tmp = tempDbPath();
  createV010Database(tmp.path);
  const db = new DatabaseSync(tmp.path);
  db.prepare("UPDATE _durableq_meta SET value = ? WHERE key = 'schema_version'").run(
    String(SCHEMA_VERSION + 1),
  );
  db.close();
  const before = schemaObjects(tmp.path);
  const q = openQueue(tmp.path);
  try {
    await assert.rejects(() => q.count(), DurableQSchemaVersionError);
    assert.deepEqual(schemaObjects(tmp.path), before);
  } finally {
    await q.close().catch(() => undefined);
    tmp.cleanup();
  }
});

test("opening an up-to-date database does not need the write lock", async () => {
  const tmp = tempDbPath();
  try {
    const q1 = openQueue(tmp.path);
    await q1.add("n", {});
    await q1.close();
    const holder = new DatabaseSync(tmp.path);
    holder.exec("BEGIN IMMEDIATE");
    try {
      const q2 = openQueue(tmp.path); // busyTimeoutMs: 50
      assert.equal(await q2.count(), 1);
      await q2.close();
    } finally {
      holder.exec("ROLLBACK");
      holder.close();
    }
  } finally {
    tmp.cleanup();
  }
});

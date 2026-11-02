import assert from "node:assert/strict";
import test from "node:test";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { createFrozenClock } from "../../src/clock.ts";
import { RETENTION_INTERVAL_MS, SCHEMA_VERSION } from "../../src/constants.ts";
import type { JobRow } from "../../src/storage/protocol.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { rawRequest } from "../support/test-control.ts";
import { createV010Database, readRows } from "../support/v010-db.ts";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");
const T0 = 1_800_000_000_000;

/** Runs the actor until it reaches its fault gate (or marker), then SIGKILLs it. */
async function killAtGate(
  dir: string,
  args: string[],
  env: Record<string, string> = {},
  gateDir = join(dir, `gate-${Math.random().toString(36).slice(2)}`),
): Promise<void> {
  mkdirSync(gateDir, { recursive: true });
  const child = spawn(
    process.execPath,
    [join(root, "tests/fixtures/processes/v020-actor.ts"), ...args, gateDir],
    { cwd: root, env: { ...process.env, ...env, DURABLEQ_ENABLE_TEST_HOOKS: "1" } },
  );
  let stderr = "";
  child.stderr.on("data", (d) => (stderr += String(d)));
  const closed = new Promise((r) => child.on("close", r));
  try {
    await eventually(() => assert.equal(existsSync(join(gateDir, "paused")), true, stderr), {
      timeoutMs: 10_000,
    });
  } finally {
    child.kill("SIGKILL");
    await closed;
  }
}

function integrity(path: string): void {
  const db = new DatabaseSync(path, { timeout: 5_000 });
  try {
    const r = db.prepare("PRAGMA integrity_check").get() as { integrity_check: string };
    assert.equal(r.integrity_check, "ok");
  } finally {
    db.close();
  }
}

test("SIGKILL inside addMany's transaction commits none of the batch", async () => {
  const tmp = tempDbPath();
  try {
    const q = openQueue(tmp.path);
    await q.add("n", { pre: true });
    await q.close();
    await killAtGate(tmp.dir, [tmp.path, "hang-enqueueMany", "x"]);
    integrity(tmp.path);
    const q2 = openQueue(tmp.path);
    assert.equal(await q2.count(), 1);
    await q2.close();
    assertDatabaseInvariants(tmp.path);
  } finally {
    tmp.cleanup();
  }
});

test("SIGKILL while materializing a schedule occurrence leaves the slot to fire later", async () => {
  const tmp = tempDbPath();
  try {
    const q = openQueue(tmp.path, "app", createFrozenClock(T0));
    await q.schedule("tick", { id: "s", everyMs: 1_000, data: null });
    await q.close();
    await killAtGate(tmp.dir, [tmp.path, "hang-schedule", String(T0 + 1_000)]);
    integrity(tmp.path);
    const q2 = openQueue(tmp.path, "app", createFrozenClock(T0));
    try {
      assert.equal(await q2.count(), 0);
      assert.equal((await q2.getSchedule("s"))?.nextRunAt, T0 + 1_000);
      const row = await rawRequest<JobRow | null>(q2, "claim", {
        nowMs: T0 + 1_000,
        queue: "app",
        workerId: "w",
        names: ["tick"],
        leaseDurationMs: 60_000,
      });
      assert.equal(row?.dedupe_key, `durableq:schedule:s:${T0 + 1_000}`);
      assert.equal(await q2.count(), 1);
    } finally {
      await q2.close();
    }
  } finally {
    tmp.cleanup();
  }
});

for (const when of ["before", "after"] as const) {
  test(`SIGKILL ${when} a progress commit keeps the last committed value`, async () => {
    const tmp = tempDbPath();
    try {
      const q = openQueue(tmp.path);
      const { job } = await q.add("n", {});
      await q.close();
      await killAtGate(tmp.dir, [tmp.path, "progress", when]);
      integrity(tmp.path);
      const q2 = openQueue(tmp.path);
      try {
        const got = await q2.get(job.id);
        assert.equal(got?.status, "active");
        assert.equal(got?.progress, 42);
      } finally {
        await q2.close();
      }
      assertDatabaseInvariants(tmp.path);
    } finally {
      tmp.cleanup();
    }
  });
}

test("SIGKILL during a retention batch deletes nothing; the next run completes", async () => {
  const tmp = tempDbPath();
  try {
    const q = openQueue(tmp.path);
    await q.addMany(Array.from({ length: 50 }, (_, i) => ({ name: "n", data: { i } })));
    await q.close();
    const raw = new DatabaseSync(tmp.path);
    raw.exec(
      `UPDATE durableq_jobs SET status = 'completed', completed_at = ${T0}, updated_at = ${T0}`,
    );
    raw.close();
    await killAtGate(tmp.dir, [tmp.path, "hang-retention", String(T0 + 1_000)]);
    integrity(tmp.path);
    assert.equal(readRows(tmp.path, "SELECT id FROM durableq_jobs").length, 50);
    const q2 = openQueue(tmp.path);
    try {
      const deleted = await rawRequest<number>(q2, "applyRetention", {
        nowMs: T0 + 1_000 + RETENTION_INTERVAL_MS,
        queue: "app",
        rules: [{ status: "completed", ageMs: 1 }],
      });
      assert.equal(deleted, 50);
    } finally {
      await q2.close();
    }
  } finally {
    tmp.cleanup();
  }
});

test("SIGKILL in the middle of a v0.1.0 upgrade rolls back; reopening upgrades cleanly", async () => {
  const tmp = tempDbPath();
  createV010Database(tmp.path);
  const before = readRows(tmp.path, "SELECT * FROM durableq_jobs ORDER BY id");
  try {
    const gate = join(tmp.dir, "migrate-gate");
    const fault = { point: "migrate:before-commit", action: "pause", gateDir: gate };
    await killAtGate(
      tmp.dir,
      [tmp.path, "pause"],
      { DURABLEQ_TEST_MIGRATE_FAULT: JSON.stringify(fault) },
      gate,
    );
    integrity(tmp.path);
    const [ver] = readRows(tmp.path, "SELECT value FROM _durableq_meta");
    assert.equal(ver?.value, "1");
    assert.deepEqual(readRows(tmp.path, "SELECT * FROM durableq_jobs ORDER BY id"), before);

    const q = openQueue(tmp.path);
    assert.equal(await q.count(), 8);
    await q.close();
    const [after] = readRows(tmp.path, "SELECT value FROM _durableq_meta");
    assert.equal(after?.value, String(SCHEMA_VERSION));
  } finally {
    tmp.cleanup();
  }
});

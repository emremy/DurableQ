import assert from "node:assert/strict";
import test from "node:test";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { SCHEMA_VERSION } from "../../src/constants.ts";
import { createFrozenClock } from "../../src/clock.ts";
import { Queue } from "../../src/queue.ts";
import { ensureTestWorker } from "../support/install-test-worker.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { createV010Database, readRows } from "../support/v010-db.ts";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");
const ACTOR = "tests/fixtures/processes/v020-actor.ts";
const T0 = 1_800_000_000_000;

function run(
  script: string,
  args: string[],
): Promise<{ stdout: string; stderr: string; code: number }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [join(root, script), ...args], {
      cwd: root,
      env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => (stdout += String(d)));
    child.stderr.on("data", (d) => (stderr += String(d)));
    child.on("error", reject);
    child.on("close", (code) => resolve({ stdout, stderr, code: code ?? 1 }));
  });
}

test("competing processes never fire the same schedule slot twice", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path, "app", createFrozenClock(T0));
  await q.schedule("tick", { id: "s", everyMs: 1_000, data: null });
  await q.close();
  try {
    const times = (offset: number) =>
      JSON.stringify(Array.from({ length: 30 }, (_, k) => T0 + (k + 1) * 1_000 + offset));
    const procs = await Promise.all(
      [0, 1, 2, 3, 5, 7].map((o) => run(ACTOR, [tmp.path, "schedule-race", times(o)])),
    );
    for (const p of procs) assert.equal(p.code, 0, p.stderr);
    const claimed = procs.flatMap((p) => JSON.parse(p.stdout) as string[]);
    assert.equal(new Set(claimed).size, claimed.length, "a job was claimed twice");
    const jobs = readRows(tmp.path, "SELECT dedupe_key FROM durableq_jobs");
    const keys = jobs.map((j) => String(j.dedupe_key));
    assert.equal(new Set(keys).size, keys.length, "duplicate occurrence");
    assert.equal(jobs.length, claimed.length);
    assert.ok(jobs.length >= 1 && jobs.length <= 30);
    assertDatabaseInvariants(tmp.path, T0);
  } finally {
    tmp.cleanup();
  }
});

test("concurrent addMany from several processes respects dedupe atomically", async () => {
  const tmp = tempDbPath();
  try {
    const procs = await Promise.all(
      [0, 500, 1_000, 1_500].map((o) => run(ACTOR, [tmp.path, "add-many", String(o)])),
    );
    for (const p of procs) assert.equal(p.code, 0, p.stderr);
    const q = openQueue(tmp.path);
    assert.equal(await q.count(), 1_500);
    await q.close();
    assertDatabaseInvariants(tmp.path);
  } finally {
    tmp.cleanup();
  }
});

test("pause in one process stops claims in others; resume releases them", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    for (let i = 0; i < 20; i++) await q.add("n", { i });
    assert.equal((await run(ACTOR, [tmp.path, "pause"])).code, 0);
    assert.equal(await q.isPaused(), true);
    const paused = await run(ACTOR, [tmp.path, "claim-loop", "300"]);
    assert.deepEqual(JSON.parse(paused.stdout), { claimed: 0 });
    await q.resume();
    const resumed = await run(ACTOR, [tmp.path, "claim-loop", "300"]);
    assert.equal(JSON.parse(resumed.stdout).claimed, 20);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("no process claims after pause() commits, even mid-race", async () => {
  const tmp = tempDbPath();
  ensureTestWorker();
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 5_000 });
  try {
    for (let b = 0; b < 5; b++) {
      await q.addMany(Array.from({ length: 1_000 }, (_, i) => ({ name: "n", data: { i } })));
    }
    const loops = [1, 2, 3].map(() => run(ACTOR, [tmp.path, "claim-loop", "1500"]));
    await new Promise((r) => setTimeout(r, 400));
    await q.pause();
    const activeAtPause = await q.count({ status: "active" });
    const results = await Promise.all(loops);
    const claimed = results.reduce((n, r) => n + JSON.parse(r.stdout).claimed, 0);
    assert.equal(await q.count({ status: "active" }), activeAtPause);
    assert.equal(claimed, activeAtPause);
    assert.ok(activeAtPause < 5_000);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("concurrent retention runs are coordinated across processes", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const out = await q.addMany(
      Array.from({ length: 300 }, (_, i) => ({ name: "n", data: { i } })),
    );
    await q.close();
    const raw = new DatabaseSync(tmp.path);
    raw.exec(
      `UPDATE durableq_jobs SET status = 'completed', completed_at = ${T0}, updated_at = ${T0}`,
    );
    raw.close();
    assert.equal(out.length, 300);
    const procs = await Promise.all(
      [1, 2, 3, 4].map(() => run(ACTOR, [tmp.path, "retention", String(T0 + 1_000)])),
    );
    for (const p of procs) assert.equal(p.code, 0, p.stderr);
    const deleted = procs.map((p) => JSON.parse(p.stdout).deleted as number).sort((a, b) => a - b);
    assert.deepEqual(deleted, [0, 0, 0, 300]);
    assert.equal(readRows(tmp.path, "SELECT * FROM durableq_jobs").length, 0);
  } finally {
    tmp.cleanup();
  }
});

test("concurrent processes upgrading the same v0.1.0 database migrate it once", async () => {
  const tmp = tempDbPath();
  createV010Database(tmp.path);
  const before = readRows(tmp.path, "SELECT * FROM durableq_jobs ORDER BY id");
  try {
    const procs = await Promise.all(
      Array.from({ length: 6 }, () => run("tests/fixtures/processes/migrate-init.ts", [tmp.path])),
    );
    for (const p of procs) assert.equal(p.code, 0, p.stderr);
    const [ver] = readRows(tmp.path, "SELECT value FROM _durableq_meta");
    assert.equal(ver?.value, String(SCHEMA_VERSION));
    const after = readRows(tmp.path, "SELECT * FROM durableq_jobs ORDER BY id");
    assert.deepEqual(
      after.map(({ progress: _p, ...rest }) => rest),
      before,
    );
  } finally {
    tmp.cleanup();
  }
});

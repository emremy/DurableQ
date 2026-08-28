import assert from "node:assert/strict";
import test from "node:test";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { rawRequest, setStorageNow, tickLeases } from "../support/test-control.ts";
import { Queue } from "../../src/queue.ts";
import { createFrozenClock } from "../../src/clock.ts";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");

function run(script: string, args: string[]): Promise<{ stdout: string; code: number }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [join(root, script), ...args], {
      cwd: root,
      env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" },
    });
    let stdout = "";
    child.stdout.on("data", (d) => {
      stdout += String(d);
    });
    child.stderr.on("data", () => {
      /* ignore child diagnostics; assert via stdout/code */
    });
    child.on("error", reject);
    child.on("close", (code) => resolve({ stdout, code: code ?? 1 }));
  });
}

test("C-001 sixteen processes claim one job once", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const { job } = await q.add("n", { v: 1 });
    await q.close();
    const procs = await Promise.all(
      Array.from({ length: 16 }, (_, i) =>
        run("tests/fixtures/processes/claim-one.ts", [tmp.path, `w-${i}`]),
      ),
    );
    const wins = procs.filter((p) => p.stdout.includes('"claimed":true'));
    assert.equal(wins.length, 1, procs.map((p) => p.stdout + p.code).join(" | "));
    const q2 = openQueue(tmp.path);
    const got = await q2.get(job.id);
    assert.equal(got?.status, "active");
    assert.equal(got?.attemptCount, 1);
    assertDatabaseInvariants(tmp.path);
    await q2.close();
  } finally {
    tmp.cleanup();
  }
});

test("C-003 dedupe stampede", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  await q.count();
  await q.close();
  try {
    const procs = await Promise.all(
      Array.from({ length: 16 }, () =>
        run("tests/fixtures/processes/enqueue-dedupe.ts", [tmp.path, "same"]),
      ),
    );
    const ids = procs.map((p) => JSON.parse(p.stdout).id);
    assert.ok(ids.every((id) => id === ids[0]));
    const q2 = openQueue(tmp.path);
    assert.equal(await q2.count(), 1);
    assertDatabaseInvariants(tmp.path);
    await q2.close();
  } finally {
    tmp.cleanup();
  }
});

test("C-005 migration stampede", async () => {
  const tmp = tempDbPath();
  try {
    const procs = await Promise.all(
      Array.from({ length: 8 }, () => run("tests/fixtures/processes/migrate-init.ts", [tmp.path])),
    );
    assert.ok(
      procs.every((p) => p.code === 0),
      procs.map((p) => p.stdout).join("\n"),
    );
    const q = openQueue(tmp.path);
    await q.count();
    assertDatabaseInvariants(tmp.path);
    await q.close();
  } finally {
    tmp.cleanup();
  }
});

test("C-002 many jobs many workers all complete", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    for (let i = 0; i < 40; i++) {
      await q.add("n", { i }, { attempts: 3, backoff: { type: "none" } });
    }
    await q.close();
    const procs = await Promise.all(
      Array.from({ length: 4 }, () =>
        run("tests/fixtures/processes/worker-run.ts", [tmp.path, "4000"]),
      ),
    );
    assert.ok(
      procs.every((p) => p.code === 0),
      procs.map((p) => `${p.code}:${p.stdout}`).join(" | "),
    );
    const q2 = openQueue(tmp.path);
    assert.equal(await q2.count({ status: "completed" }), 40);
    assertDatabaseInvariants(tmp.path);
    await q2.close();
  } finally {
    tmp.cleanup();
  }
});

test("C-006 stale generation cannot complete after newer claim", async () => {
  const tmp = tempDbPath();
  const t0 = 3_000_000;
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 }, createFrozenClock(t0));
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  try {
    await setStorageNow(q, t0);
    await q.add("n", { v: 1 }, { attempts: 3 });
    const a = (await rawRequest(q, "claim", {
      nowMs: t0,
      queue: q.name,
      workerId: "w1",
      names: ["n"],
      leaseDurationMs: 50,
    })) as { id: string; lease_generation: number };
    await setStorageNow(q, t0 + 50);
    await tickLeases(q);
    const b = (await rawRequest(q, "claim", {
      nowMs: t0 + 50,
      queue: q.name,
      workerId: "w2",
      names: ["n"],
      leaseDurationMs: 5_000,
    })) as { lease_generation: number };
    assert.equal(b.lease_generation, 2);
    await Promise.allSettled([
      rawRequest(q, "settleSuccess", {
        nowMs: t0 + 50,
        id: a.id,
        workerId: "w1",
        generation: 1,
        resultJson: "{}",
      }),
      rawRequest(q, "settleSuccess", {
        nowMs: t0 + 50,
        id: a.id,
        workerId: "w2",
        generation: 2,
        resultJson: "{}",
      }),
    ]);
    const got = await q.get(a.id);
    assert.equal(got?.status, "completed");
    assert.equal(got?.leaseGeneration, 2);
    assertDatabaseInvariants(tmp.path, t0 + 50);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("C-007 heartbeat vs recovery near expiry", async () => {
  const tmp = tempDbPath();
  const t0 = 4_000_000;
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 }, createFrozenClock(t0));
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  try {
    await setStorageNow(q, t0);
    await q.add("n", { v: 1 }, { attempts: 3 });
    const claimed = (await rawRequest(q, "claim", {
      nowMs: t0,
      queue: q.name,
      workerId: "w1",
      names: ["n"],
      leaseDurationMs: 10,
    })) as { id: string; lease_generation: number };
    await setStorageNow(q, t0 + 10);
    const [hb] = await Promise.all([
      rawRequest(q, "heartbeat", {
        nowMs: t0 + 10,
        id: claimed.id,
        workerId: "w1",
        generation: claimed.lease_generation,
        leaseDurationMs: 10,
      }),
      rawRequest(q, "recoverExpired", { nowMs: t0 + 10 }),
    ]);
    assert.equal(hb, false);
    const got = await q.get(claimed.id);
    assert.ok(got);
    assert.notEqual(got.status, "active");
    assert.notEqual(got.status, "completed");
    assertDatabaseInvariants(tmp.path, t0 + 10);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

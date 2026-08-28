import assert from "node:assert/strict";
import test from "node:test";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { createFrozenClock } from "../../src/clock.ts";
import { DurableQStaleLeaseError } from "../../src/errors.ts";
import { Queue } from "../../src/queue.ts";
import { registrySize } from "../../src/storage/runtime-registry.ts";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { configureFault, rawRequest, setStorageNow, tickLeases } from "../support/test-control.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

function integrityOk(path: string): void {
  const db = new DatabaseSync(path, { timeout: 2000, defensive: true, allowExtension: false });
  try {
    const row = db.prepare("PRAGMA integrity_check").get() as { integrity_check: string };
    assert.equal(row.integrity_check, "ok", JSON.stringify(row));
  } finally {
    db.close();
  }
}

test("D-004 dedupe reserved after complete and failed until clean", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const a = await q.add("n", { v: 1 }, { dedupeKey: "done" });
    const claimed = (await rawRequest(q, "claim", {
      nowMs: Date.now(),
      queue: q.name,
      workerId: "w",
      names: ["n"],
      leaseDurationMs: 30_000,
    })) as { id: string; lease_generation: number };
    await rawRequest(q, "settleSuccess", {
      nowMs: Date.now(),
      id: claimed.id,
      workerId: "w",
      generation: claimed.lease_generation,
      resultJson: "{}",
    });
    const again = await q.add("n", { v: 99 }, { dedupeKey: "done" });
    assert.equal(again.deduplicated, true);
    assert.equal(again.job.id, a.job.id);
    assert.deepEqual(again.job.data, { v: 1 });
    assert.equal(again.job.status, "completed");

    const f = await q.add("n", { v: 2 }, { dedupeKey: "failk", attempts: 1 });
    const c2 = (await rawRequest(q, "claim", {
      nowMs: Date.now(),
      queue: q.name,
      workerId: "w",
      names: ["n"],
      leaseDurationMs: 30_000,
    })) as { id: string; lease_generation: number };
    await rawRequest(q, "settleFailure", {
      nowMs: Date.now(),
      id: c2.id,
      workerId: "w",
      generation: c2.lease_generation,
      error: { name: "Error", message: "x" },
      nonRetryable: true,
    });
    const againFail = await q.add("n", { v: 3 }, { dedupeKey: "failk" });
    assert.equal(againFail.deduplicated, true);
    assert.equal(againFail.job.id, f.job.id);
    const retried = await q.retry(f.job.id);
    assert.equal(retried.retried, true);
    const afterRetry = await q.add("n", { v: 4 }, { dedupeKey: "failk" });
    assert.equal(afterRetry.deduplicated, true);
    assert.equal(afterRetry.job.id, f.job.id);
    await q.clean({ status: "completed", before: Date.now() + 1000 });
    const fresh = await q.add("n", { v: 5 }, { dedupeKey: "done" });
    assert.equal(fresh.deduplicated, false);
    assertDatabaseInvariants(tmp.path);
    integrityOk(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("INV-005 stale heartbeat cannot extend a newer generation", async () => {
  const tmp = tempDbPath();
  const t0 = 7_000_000;
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 }, createFrozenClock(t0));
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  try {
    await setStorageNow(q, t0);
    await q.add("n", { v: 1 }, { attempts: 3 });
    await rawRequest(q, "claim", {
      nowMs: t0,
      queue: q.name,
      workerId: "a",
      names: ["n"],
      leaseDurationMs: 20,
    });
    await setStorageNow(q, t0 + 20);
    await tickLeases(q);
    const b = (await rawRequest(q, "claim", {
      nowMs: t0 + 20,
      queue: q.name,
      workerId: "b",
      names: ["n"],
      leaseDurationMs: 5_000,
    })) as { id: string; lease_generation: number; lease_expires_at: number };
    assert.equal(b.lease_generation, 2);
    const hbOld = await rawRequest(q, "heartbeat", {
      nowMs: t0 + 20,
      id: b.id,
      workerId: "a",
      generation: 1,
      leaseDurationMs: 5_000,
    });
    assert.equal(hbOld, false);
    const after = await q.get(b.id);
    assert.equal(after?.leaseExpiresAt, b.lease_expires_at);
    assert.equal(after?.leaseOwner, "b");
    await assert.rejects(
      () =>
        rawRequest(q, "settleFailure", {
          nowMs: t0 + 20,
          id: b.id,
          workerId: "a",
          generation: 1,
          error: { name: "Error", message: "stale" },
          nonRetryable: false,
        }),
      DurableQStaleLeaseError,
    );
    assert.equal((await q.get(b.id))?.status, "active");
    assertDatabaseInvariants(tmp.path, t0 + 20);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("INV-026 settleFailure at expiry equality is stale", async () => {
  const tmp = tempDbPath();
  const t0 = 8_000_000;
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 }, createFrozenClock(t0));
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  try {
    await setStorageNow(q, t0);
    await q.add("n", { v: 1 });
    const claimed = (await rawRequest(q, "claim", {
      nowMs: t0,
      queue: q.name,
      workerId: "w",
      names: ["n"],
      leaseDurationMs: 100,
    })) as { id: string; lease_generation: number; lease_expires_at: number };
    await assert.rejects(
      () =>
        rawRequest(q, "settleFailure", {
          nowMs: claimed.lease_expires_at,
          id: claimed.id,
          workerId: "w",
          generation: 1,
          error: { name: "Error", message: "late" },
          nonRetryable: false,
        }),
      DurableQStaleLeaseError,
    );
    assert.equal((await q.get(claimed.id))?.status, "active");
    assertDatabaseInvariants(tmp.path, claimed.lease_expires_at);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("claim opportunistically recovers expired lease in the same transaction", async () => {
  const tmp = tempDbPath();
  const t0 = 9_000_000;
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 }, createFrozenClock(t0));
  process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
  try {
    await setStorageNow(q, t0);
    await q.add("n", { v: 1 }, { attempts: 3, backoff: { type: "none" } });
    await rawRequest(q, "claim", {
      nowMs: t0,
      queue: q.name,
      workerId: "dead",
      names: ["n"],
      leaseDurationMs: 10,
    });
    await setStorageNow(q, t0 + 10);
    const next = (await rawRequest(q, "claim", {
      nowMs: t0 + 10,
      queue: q.name,
      workerId: "live",
      names: ["n"],
      leaseDurationMs: 5_000,
    })) as { lease_generation: number; attempt_count: number; lease_owner: string };
    assert.equal(next.lease_owner, "live");
    assert.equal(next.lease_generation, 2);
    assert.equal(next.attempt_count, 2);
    assertDatabaseInvariants(tmp.path, t0 + 10);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("D-006 claim F4 leaves an active generation the caller never observed", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const { job } = await q.add("only", { i: 1 });
    await configureFault(q, { point: "claim:after-commit", action: "throw" });
    await assert.rejects(() =>
      rawRequest(q, "claim", {
        nowMs: Date.now(),
        queue: q.name,
        workerId: "w1",
        names: ["only"],
        leaseDurationMs: 30_000,
      }),
    );
    await configureFault(q, { point: "claim:after-commit", action: "none" });
    const got = await q.get(job.id);
    assert.equal(got?.status, "active");
    assert.equal(got?.leaseOwner, "w1");
    assert.equal(got?.leaseGeneration, 1);
    assert.equal(got?.attemptCount, 1);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("D-003 short writer lock still allows an unexpired heartbeat", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { v: 1 });
    const nowClaim = Date.now();
    const claimed = (await rawRequest(q, "claim", {
      nowMs: nowClaim,
      queue: q.name,
      workerId: "w1",
      names: ["n"],
      leaseDurationMs: 5_000,
    })) as { id: string; lease_generation: number; lease_expires_at: number };
    const locker = new DatabaseSync(tmp.path, { timeout: 1 });
    locker.exec("BEGIN IMMEDIATE");
    await new Promise((r) => setTimeout(r, 30));
    locker.exec("COMMIT");
    locker.close();
    const hb = await rawRequest(q, "heartbeat", {
      nowMs: Date.now(),
      id: claimed.id,
      workerId: "w1",
      generation: claimed.lease_generation,
      leaseDurationMs: 5_000,
    });
    assert.equal(hb, true);
    const after = await q.get(claimed.id);
    assert.ok(after?.leaseExpiresAt && after.leaseExpiresAt > claimed.lease_expires_at);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("queue.close before worker.close must not leak the storage runtime", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    {
      n: async () => {
        await new Promise((r) => setTimeout(r, 80));
        return { ok: true };
      },
    },
    { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    await q.add("n", { v: 1 });
    await worker.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "active" }), 1);
    });
    const sizeWhileRunning = registrySize();
    assert.ok(sizeWhileRunning >= 1);
    await q.close();
    await worker.close({ timeoutMs: 2_000 });
    assert.equal(registrySize(), 0, "storage runtime leaked after queue.close then worker.close");
  } finally {
    await worker.close({ timeoutMs: 200 }).catch(() => undefined);
    await q.close().catch(() => undefined);
    tmp.cleanup();
  }
});

test("INV-019 shutdown continues lease renewal until the in-flight handler finishes", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    {
      slow: async () => {
        await new Promise((r) => setTimeout(r, 350));
        return { ok: true };
      },
    },
    { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 200, heartbeatIntervalMs: 50 },
  );
  try {
    await q.add("slow", { n: 1 });
    await worker.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "active" }), 1);
    });
    await worker.close({ timeoutMs: 2_000 });
    assert.equal(await q.count({ status: "completed" }), 1);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("D-006 settleFailure F1-F3 do not persist; F4 is durable while the caller fails", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    for (const point of [
      "settleFailure:before-begin",
      "settleFailure:after-begin",
      "settleFailure:before-commit",
    ] as const) {
      const { job } = await q.add("n", { v: point }, { attempts: 2, backoff: { type: "none" } });
      const claimed = (await rawRequest(q, "claim", {
        nowMs: Date.now(),
        queue: q.name,
        workerId: "w1",
        names: ["n"],
        leaseDurationMs: 30_000,
      })) as { id: string; lease_generation: number };
      await configureFault(q, { point, action: "throw" });
      await assert.rejects(() =>
        rawRequest(q, "settleFailure", {
          nowMs: Date.now(),
          id: claimed.id,
          workerId: "w1",
          generation: claimed.lease_generation,
          error: { name: "Error", message: point },
          nonRetryable: false,
        }),
      );
      await configureFault(q, { point, action: "none" });
      const still = await q.get(job.id);
      assert.equal(still?.status, "active", point);
    }

    const { job } = await q.add("n", { v: "f4" }, { attempts: 1, backoff: { type: "none" } });
    const claimed = (await rawRequest(q, "claim", {
      nowMs: Date.now(),
      queue: q.name,
      workerId: "w1",
      names: ["n"],
      leaseDurationMs: 30_000,
    })) as { id: string; lease_generation: number };
    await configureFault(q, { point: "settleFailure:after-commit", action: "throw" });
    await assert.rejects(() =>
      rawRequest(q, "settleFailure", {
        nowMs: Date.now(),
        id: claimed.id,
        workerId: "w1",
        generation: claimed.lease_generation,
        error: { name: "Error", message: "f4" },
        nonRetryable: true,
      }),
    );
    await configureFault(q, { point: "settleFailure:after-commit", action: "none" });
    assert.equal((await q.get(job.id))?.status, "failed");
    await assert.rejects(
      () =>
        rawRequest(q, "settleFailure", {
          nowMs: Date.now(),
          id: claimed.id,
          workerId: "w1",
          generation: claimed.lease_generation,
          error: { name: "Error", message: "again" },
          nonRetryable: true,
        }),
      DurableQStaleLeaseError,
    );
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("INV-002 add Promise stays pending until COMMIT", async () => {
  const tmp = tempDbPath();
  const gateDir = join(tmp.dir, "gate");
  mkdirSync(gateDir, { recursive: true });
  const q = openQueue(tmp.path);
  try {
    await configureFault(q, { point: "enqueue:before-commit", action: "pause", gateDir });
    let resolved = false;
    const pending = q.add("n", { v: 1 }).then((result) => {
      resolved = true;
      return result;
    });
    await eventually(() => {
      assert.equal(existsSync(join(gateDir, "paused")), true);
    });
    await new Promise((r) => setTimeout(r, 50));
    assert.equal(resolved, false, "add() resolved before COMMIT");
    writeFileSync(join(gateDir, "resume"), "1");
    const result = await pending;
    assert.equal(resolved, true);
    assert.equal(result.deduplicated, false);
    assert.equal(await q.count(), 1);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("production storage worker rejects test operations", async () => {
  const tmp = tempDbPath();
  const root = join(dirname(fileURLToPath(import.meta.url)), "../..");
  try {
    const child = spawn(
      process.execPath,
      [join(root, "tests/fixtures/processes/hooks-off.ts"), tmp.path],
      {
        cwd: root,
        env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "" },
      },
    );
    const stdout = await new Promise<string>((resolve, reject) => {
      let out = "";
      child.stdout.on("data", (d) => {
        out += String(d);
      });
      child.on("error", reject);
      child.on("close", (code) => {
        if (code !== 0) reject(new Error(`hooks-off exited ${code}: ${out}`));
        else resolve(out);
      });
    });
    const parsed = JSON.parse(stdout.trim().split("\n").at(-1) ?? "{}") as {
      ok: boolean;
      name?: string;
    };
    assert.equal(parsed.ok, true);
    assert.equal(parsed.name, "DurableQValidationError");
  } finally {
    tmp.cleanup();
  }
});

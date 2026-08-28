import assert from "node:assert/strict";
import test from "node:test";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { configureFault, tickLeases } from "../support/test-control.ts";
import { Worker } from "../../src/worker.ts";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");

test("CR-001 queued durability after SIGKILL", async () => {
  const tmp = tempDbPath();
  try {
    const child = spawn(
      process.execPath,
      [join(root, "tests/fixtures/processes/enqueue-and-hang.ts"), tmp.path],
      {
        cwd: root,
        env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" },
      },
    );
    const id = await new Promise<string>((resolve, reject) => {
      child.stdout.on("data", (d) => {
        const line = String(d).trim().split("\n").at(-1);
        if (!line) return;
        resolve(JSON.parse(line).id);
      });
      child.on("error", reject);
    });
    child.kill("SIGKILL");
    await new Promise((r) => child.on("close", r));
    const q = openQueue(tmp.path);
    const job = await q.get(id);
    assert.equal(job?.status, "queued");
    assertDatabaseInvariants(tmp.path);
    await q.close();
  } finally {
    tmp.cleanup();
  }
});

test("CR-002 crash after claim recovers after lease expiry", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { v: 1 }, { attempts: 3, backoff: { type: "none" } });
    const child = spawn(
      process.execPath,
      [join(root, "tests/fixtures/processes/worker-hang.ts"), tmp.path, "hang"],
      { cwd: root, env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" } },
    );
    await new Promise<void>((resolve, reject) => {
      child.stdout.on("data", () => resolve());
      child.on("error", reject);
    });
    child.kill("SIGKILL");
    await new Promise((r) => child.on("close", r));
    const worker = new Worker(
      q,
      { n: async () => ({ recovered: true }) },
      { concurrency: 1, pollIntervalMs: 30, leaseDurationMs: 400, heartbeatIntervalMs: 50 },
    );
    await worker.start();
    await eventually(
      async () => {
        assert.equal(await q.count({ status: "completed" }), 1);
      },
      { timeoutMs: 8_000 },
    );
    await worker.close({ timeoutMs: 500 });
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("CR-003 crash during handler recovers from marker", async () => {
  const tmp = tempDbPath();
  const marker = join(tmp.dir, "started.json");
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { v: 1 }, { attempts: 3, backoff: { type: "none" } });
    const child = spawn(
      process.execPath,
      [join(root, "tests/fixtures/processes/worker-hang.ts"), tmp.path, "hang", marker],
      { cwd: root, env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" } },
    );
    await eventually(() => {
      assert.equal(existsSync(marker), true);
    });
    child.kill("SIGKILL");
    await new Promise((r) => child.on("close", r));
    const worker = new Worker(
      q,
      { n: async () => ({ recovered: true }) },
      { concurrency: 1, pollIntervalMs: 30, leaseDurationMs: 400, heartbeatIntervalMs: 50 },
    );
    await worker.start();
    await eventually(
      async () => {
        assert.equal(await q.count({ status: "completed" }), 1);
      },
      { timeoutMs: 8_000 },
    );
    await worker.close({ timeoutMs: 500 });
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("CR-005 kill during enqueue after commit converges on dedupe", async () => {
  const tmp = tempDbPath();
  try {
    const child = spawn(
      process.execPath,
      [join(root, "tests/fixtures/processes/enqueue-after-commit-hang.ts"), tmp.path, "amb"],
      { cwd: root, env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" } },
    );
    await eventually(() => {
      if (!existsSync(tmp.path)) throw new Error("no db yet");
      const db = new DatabaseSync(tmp.path, { timeout: 50 });
      try {
        const row = db.prepare("SELECT COUNT(*) AS n FROM durableq_jobs").get() as { n: number };
        assert.equal(Number(row.n) >= 1, true);
      } finally {
        db.close();
      }
    });
    child.kill("SIGKILL");
    await new Promise((r) => child.on("close", r));
    const q = openQueue(tmp.path);
    const again = await q.add("n", { v: 99 }, { dedupeKey: "amb" });
    assert.equal(again.deduplicated, true);
    assert.equal(await q.count(), 1);
    assertDatabaseInvariants(tmp.path);
    await q.close();
  } finally {
    tmp.cleanup();
  }
});

test("CR-006 kill storage worker thread rejects outstanding RPCs", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.count();
    await configureFault(q, { point: "enqueue:before-begin", action: "exit" });
    await assert.rejects(() => q.add("x", { a: 1 }), /storage worker/i);
  } finally {
    await q.close().catch(() => undefined);
    tmp.cleanup();
  }
});

test("CR-007 repeated crash recovery reaches terminal failure", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    const { job } = await q.add("n", { v: 1 }, { attempts: 2, backoff: { type: "none" } });
    for (let i = 0; i < 2; i++) {
      const child = spawn(
        process.execPath,
        [join(root, "tests/fixtures/processes/worker-hang.ts"), tmp.path, "hang"],
        { cwd: root, env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" } },
      );
      await new Promise<void>((resolve, reject) => {
        child.stdout.on("data", () => resolve());
        child.on("error", reject);
      });
      child.kill("SIGKILL");
      await new Promise((r) => child.on("close", r));
      await new Promise((r) => setTimeout(r, 500));
      await tickLeases(q);
    }
    const got = await q.get(job.id);
    assert.equal(got?.status, "failed");
    assert.equal(got?.attemptCount, 2);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("CR-004 crash after side effect before ACK is at-least-once", async () => {
  const tmp = tempDbPath();
  const seen: string[] = [];
  const logical = new Set<string>();
  const server = createServer((req, res) => {
    const key = String(req.headers["idempotency-key"] ?? "");
    seen.push(key);
    logical.add(key);
    res.end("ok");
  });
  await new Promise<void>((r) => server.listen(0, r));
  const port = (server.address() as { port: number }).port;
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { v: 1 }, { attempts: 5, backoff: { type: "none" } });
    const child = spawn(
      process.execPath,
      [
        join(root, "tests/fixtures/processes/worker-side-effect.ts"),
        tmp.path,
        `http://127.0.0.1:${port}/`,
      ],
      { cwd: root, env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" } },
    );
    await eventually(() => {
      assert.ok(seen.length >= 1);
    });
    child.kill("SIGKILL");
    await new Promise((r) => child.on("close", r));
    const worker = new Worker(
      q,
      {
        n: async (job, ctx) => {
          const res = await fetch(`http://127.0.0.1:${port}/`, {
            signal: ctx.signal,
            headers: { "idempotency-key": job.id },
          });
          if (!res.ok) throw new Error("retry");
          return { ok: true };
        },
      },
      { concurrency: 1, pollIntervalMs: 30, leaseDurationMs: 400, heartbeatIntervalMs: 50 },
    );
    await worker.start();
    await eventually(
      async () => {
        assert.equal(await q.count({ status: "completed" }), 1);
      },
      { timeoutMs: 8_000 },
    );
    assert.ok(seen.length >= 2, "handler ran more than once");
    assert.equal(logical.size, 1);
    await worker.close({ timeoutMs: 500 });
    assertDatabaseInvariants(tmp.path);
  } finally {
    server.close();
    await q.close();
    tmp.cleanup();
  }
});

function pragmaIntegrity(dbPath: string): void {
  const db = new DatabaseSync(dbPath, { timeout: 5000, defensive: true, allowExtension: false });
  try {
    const integrity = db.prepare("PRAGMA integrity_check").get() as { integrity_check: string };
    assert.equal(integrity.integrity_check, "ok", JSON.stringify(integrity));
    const fk = db.prepare("PRAGMA foreign_key_check").all() as unknown[];
    assert.equal(fk.length, 0, JSON.stringify(fk));
  } finally {
    db.close();
  }
}

for (const mode of ["enqueue", "claim", "settle"] as const) {
  test(`SIGKILL during ${mode} before COMMIT rolls back the open transaction`, async () => {
    const tmp = tempDbPath();
    const gateDir = join(tmp.dir, "gate");
    mkdirSync(gateDir, { recursive: true });
    const child = spawn(
      process.execPath,
      [join(root, "tests/fixtures/processes/kill-before-commit.ts"), tmp.path, mode, gateDir],
      { cwd: root },
    );
    try {
      await eventually(
        () => {
          assert.equal(existsSync(join(gateDir, "paused")), true);
        },
        { timeoutMs: 10_000 },
      );
      child.kill("SIGKILL");
      await new Promise((r) => child.on("close", r));
      pragmaIntegrity(tmp.path);
      const q = openQueue(tmp.path);
      try {
        if (mode === "enqueue") {
          assert.equal(await q.count(), 0);
        } else if (mode === "claim") {
          assert.equal(await q.count({ status: "queued" }), 1);
          assert.equal(await q.count({ status: "active" }), 0);
        } else {
          assert.equal(await q.count({ status: "active" }), 1);
          assert.equal(await q.count({ status: "completed" }), 0);
        }
        assertDatabaseInvariants(tmp.path);
      } finally {
        await q.close();
      }
    } finally {
      tmp.cleanup();
    }
  });
}

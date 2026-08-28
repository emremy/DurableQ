import assert from "node:assert/strict";
import test from "node:test";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

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
    child.on("error", reject);
    child.on("close", (code) => resolve({ stdout, code: code ?? 1 }));
  });
}

test("RW-002 idempotent email-like provider survives crash after accept", async () => {
  const tmp = tempDbPath();
  const requests: string[] = [];
  const delivered = new Set<string>();
  const server = createServer((req, res) => {
    const key = String(req.headers["idempotency-key"] ?? "");
    requests.push(key);
    delivered.add(key);
    res.end("accepted");
  });
  await new Promise<void>((r) => server.listen(0, r));
  const port = (server.address() as { port: number }).port;
  const q = openQueue(tmp.path);
  try {
    await q.add("n", { to: "a@b.c" }, { attempts: 5, backoff: { type: "none" } });
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
      assert.ok(requests.length >= 1);
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
          return { messageId: job.id };
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
    assert.ok(requests.length >= 2);
    assert.equal(delivered.size, 1);
    await worker.close({ timeoutMs: 500 });
    assertDatabaseInvariants(tmp.path);
  } finally {
    server.close();
    await q.close();
    tmp.cleanup();
  }
});

test("RW-003 delayed report does not run early after restart", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    await q.add("report", { userId: 1 }, { delayMs: 400 });
    await q.close();
    await new Promise((r) => setTimeout(r, 80));
    const q2 = openQueue(tmp.path);
    const client = await q2.storage();
    const early = await client.request("claim", {
      nowMs: Date.now(),
      queue: q2.name,
      workerId: "w",
      names: ["report"],
      leaseDurationMs: 5_000,
    });
    assert.equal(early, null);
    const worker = new Worker(
      q2,
      { report: async () => ({ path: "/tmp/r" }) },
      { concurrency: 1, pollIntervalMs: 30, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
    );
    await worker.start();
    await eventually(
      async () => {
        assert.equal(await q2.count({ status: "completed" }), 1);
      },
      { timeoutMs: 5_000 },
    );
    await worker.close({ timeoutMs: 500 });
    assertDatabaseInvariants(tmp.path);
    await q2.close();
  } finally {
    tmp.cleanup();
  }
});

test("RW-004 graceful deployment restart continues remaining work", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  let started = 0;
  const worker = new Worker(
    q,
    {
      n: async () => {
        started += 1;
        await new Promise((r) => setTimeout(r, 60));
        return { ok: true };
      },
    },
    { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    await q.add("n", { i: 1 });
    await q.add("n", { i: 2 });
    await worker.start();
    await eventually(() => {
      assert.equal(started, 1);
    });
    await worker.close({ timeoutMs: 2_000 });
    assert.equal(started, 1);
    assert.equal(await q.count({ status: "completed" }), 1);
    assert.equal(await q.count({ status: "queued" }), 1);
    const worker2 = new Worker(
      q,
      { n: async () => ({ ok: true }) },
      { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
    );
    await worker2.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "completed" }), 2);
    });
    await worker2.close({ timeoutMs: 500 });
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("RW-005 multi-process web plus worker topology", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  await q.count();
  await q.close();
  try {
    const workers = [
      spawn(
        process.execPath,
        [join(root, "tests/fixtures/processes/worker-run.ts"), tmp.path, "5000"],
        {
          cwd: root,
          env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" },
        },
      ),
      spawn(
        process.execPath,
        [join(root, "tests/fixtures/processes/worker-run.ts"), tmp.path, "5000"],
        {
          cwd: root,
          env: { ...process.env, DURABLEQ_ENABLE_TEST_HOOKS: "1" },
        },
      ),
    ];
    const producers = await Promise.all([
      run("tests/fixtures/processes/producer.ts", [tmp.path, "8", "a"]),
      run("tests/fixtures/processes/producer.ts", [tmp.path, "8", "b"]),
    ]);
    assert.ok(
      producers.every((p) => p.code === 0),
      producers.map((p) => p.stdout).join(" | "),
    );
    await Promise.all(workers.map((w) => new Promise((r) => w.on("close", r))));
    const q2 = openQueue(tmp.path);
    const terminal =
      (await q2.count({ status: "completed" })) + (await q2.count({ status: "failed" }));
    assert.equal(terminal, 16);
    assertDatabaseInvariants(tmp.path);
    await q2.close();
  } finally {
    tmp.cleanup();
  }
});

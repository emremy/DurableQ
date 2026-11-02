// DurableQ benchmark. Runs against any source tree with the v0.1.0+ layout, so the same
// workloads can be compared across versions:
//   node bench/benchmark.ts                      # this checkout
//   node bench/benchmark.ts --src /tmp/dq010/src  # e.g. a `git worktree` of v0.1.0
// Features missing from the target version are reported as "n/a".
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, statSync, existsSync, writeFileSync } from "node:fs";
import { cpus, tmpdir, totalmem } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { PAYLOAD, seed } from "./seed.ts";

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) {
  args.set(process.argv[i]!.replace(/^--/, ""), process.argv[i + 1] ?? "");
}
const src = resolve(args.get("src") ?? new URL("../src", import.meta.url).pathname);
const out = args.get("out");
const scale = Number(args.get("scale") ?? "1");
const n = (x: number) => Math.max(1, Math.round(x * scale));

// Typed as the current version; older trees lack some methods, which is checked at runtime.
type Q = InstanceType<typeof import("../src/queue.ts").Queue>;
type JobRow = import("../src/storage/protocol.ts").JobRow;
const { Queue } = (await import(
  pathToFileURL(join(src, "queue.ts")).href
)) as typeof import("../src/queue.ts");
const { Worker } = (await import(
  pathToFileURL(join(src, "worker.ts")).href
)) as typeof import("../src/worker.ts");
const { acquireRuntime } = (await import(
  pathToFileURL(join(src, "storage/runtime-registry.ts")).href
)) as typeof import("../src/storage/runtime-registry.ts");

const BUSY = 5_000;
const results: Record<string, unknown> = {};

function pct(sorted: number[], p: number): number {
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))]!;
}
function latency(samples: number[]) {
  const s = [...samples].sort((a, b) => a - b);
  const r = (x: number) => Math.round(x * 1000) / 1000;
  return { p50: r(pct(s, 50)), p95: r(pct(s, 95)), p99: r(pct(s, 99)) };
}
function fileBytes(path: string): number {
  return [path, `${path}-wal`].reduce((t, p) => t + (existsSync(p) ? statSync(p).size : 0), 0);
}
function files(path: string) {
  const size = (p: string) => (existsSync(p) ? statSync(p).size : 0);
  return { mainBytes: size(path), walBytes: size(`${path}-wal`) };
}

async function measure<T>(fn: () => Promise<T>) {
  const cpu0 = process.cpuUsage();
  let peakRss = process.memoryUsage().rss;
  const sampler = setInterval(() => {
    peakRss = Math.max(peakRss, process.memoryUsage().rss);
  }, 20);
  const t0 = performance.now();
  const value = await fn();
  const ms = performance.now() - t0;
  clearInterval(sampler);
  const cpu = process.cpuUsage(cpu0);
  return {
    value,
    ms,
    cpuMs: Math.round((cpu.user + cpu.system) / 1000),
    peakRssMb: Math.round(peakRss / 1e6),
  };
}

async function withDb<T>(fn: (path: string) => Promise<T>): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), "dq-bench-"));
  try {
    return await fn(join(dir, "bench.db"));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

async function enqueue(q: Q, count: number): Promise<void> {
  if (typeof q.addMany === "function") {
    for (let i = 0; i < count; i += 1_000) {
      const len = Math.min(1_000, count - i);
      await q.addMany(Array.from({ length: len }, () => ({ name: "job", data: PAYLOAD })));
    }
  } else {
    for (let i = 0; i < count; i++) await q.add("job", PAYLOAD);
  }
}

async function benchEnqueueSingle() {
  return withDb(async (path) => {
    const q = new Queue("bench", { path, busyTimeoutMs: BUSY });
    await q.count();
    const count = n(2_000);
    const samples: number[] = [];
    const m = await measure(async () => {
      for (let i = 0; i < count; i++) {
        const t = performance.now();
        await q.add("job", PAYLOAD);
        samples.push(performance.now() - t);
      }
    });
    await q.close();
    return {
      jobs: count,
      jobsPerSec: Math.round(count / (m.ms / 1000)),
      latencyMs: latency(samples),
      cpuMs: m.cpuMs,
      peakRssMb: m.peakRssMb,
      dbBytesPerJob: Math.round(fileBytes(path) / count),
    };
  });
}

async function benchEnqueueBulk() {
  return withDb(async (path) => {
    const q = new Queue("bench", { path, busyTimeoutMs: BUSY });
    if (typeof q.addMany !== "function") return "n/a";
    await q.count();
    const count = n(50_000);
    const m = await measure(() => enqueue(q, count));
    await q.close();
    return {
      jobs: count,
      batchSize: 1_000,
      jobsPerSec: Math.round(count / (m.ms / 1000)),
      cpuMs: m.cpuMs,
      peakRssMb: m.peakRssMb,
      dbBytesPerJob: Math.round(fileBytes(path) / count),
    };
  });
}

async function runWorker(
  q: Q,
  count: number,
  handler: (job: unknown, ctx: import("../src/types.ts").JobContext) => Promise<null>,
) {
  let done = 0;
  let busyErrors = 0;
  const worker = new Worker(q, { job: handler }, { concurrency: 8, pollIntervalMs: 5 });
  worker.on("error", () => (busyErrors += 1));
  const finished = new Promise<void>((r) =>
    worker.on("completed", () => {
      if (++done === count) r();
    }),
  );
  const m = await measure(async () => {
    await worker.start();
    await finished;
  });
  await worker.close({ timeoutMs: 5_000 });
  return { m, busyErrors };
}

async function benchWorkerSingle() {
  return withDb(async (path) => {
    const q = new Queue("bench", { path, busyTimeoutMs: BUSY });
    const count = n(5_000);
    await q.count();
    seed(path, count, "queued", Date.now() - 1_000);
    const { m, busyErrors } = await runWorker(q, count, async () => null);
    await q.close();
    return {
      jobs: count,
      concurrency: 8,
      jobsPerSec: Math.round(count / (m.ms / 1000)),
      cpuMs: m.cpuMs,
      peakRssMb: m.peakRssMb,
      errors: busyErrors,
    };
  });
}

async function benchProgress() {
  return withDb(async (path) => {
    const q = new Queue("bench", { path, busyTimeoutMs: BUSY });
    const count = n(2_000);
    await q.count();
    seed(path, count, "queued", Date.now() - 1_000);
    if (typeof q.list !== "function") {
      // v0.1.0 has no progress API.
      await q.close();
      return "n/a";
    }
    const samples: number[] = [];
    const { m } = await runWorker(q, count, async (_job, ctx) => {
      for (const p of [25, 50, 75, 100]) {
        const t = performance.now();
        await ctx.updateProgress(p);
        samples.push(performance.now() - t);
      }
      return null;
    });
    await q.close();
    return {
      jobs: count,
      updatesPerJob: 4,
      jobsPerSec: Math.round(count / (m.ms / 1000)),
      updateLatencyMs: latency(samples),
      cpuMs: m.cpuMs,
    };
  });
}

function spawnChild(script: string, argv: string[]) {
  const p = spawn(process.execPath, [script, ...argv], { stdio: ["pipe", "pipe", "inherit"] });
  let text = "";
  const closed = new Promise<number | null>((r) => p.on("close", r));
  const ready = new Promise<void>((resolveReady, reject) => {
    p.stdout.on("data", (d) => {
      text += String(d);
      if (text.includes("ready\n")) resolveReady();
    });
    p.on("error", reject);
    void closed.then((code) => reject(new Error(`child exited (${code}) before ready`)));
  });
  const done = closed.then((code) => {
    if (code !== 0) throw new Error(`child exit ${code}`);
    return JSON.parse(text.trim().split("\n").at(-1)!) as { errors: number };
  });
  return { p, ready, done };
}

async function benchWorkerMulti() {
  return withDb(async (path) => {
    const q = new Queue("bench", { path, busyTimeoutMs: BUSY });
    const procs = 4;
    const count = n(20_000);
    await q.count();
    seed(path, count, "queued", Date.now() - 1_000);
    await q.close();
    const script = new URL("./worker-child.ts", import.meta.url).pathname;
    const children = Array.from({ length: procs }, () =>
      spawnChild(script, [src, path, String(count)]),
    );
    try {
      await Promise.all(children.map((c) => c.ready));
      const t0 = performance.now();
      for (const c of children) c.p.stdin.write("go\n");
      const results = await Promise.all(children.map((c) => c.done));
      const ms = performance.now() - t0;
      return {
        jobs: count,
        processes: procs,
        concurrencyPerProcess: 8,
        jobsPerSec: Math.round(count / (ms / 1000)),
        workerErrors: results.reduce((t, c) => t + c.errors, 0),
      };
    } finally {
      for (const c of children) if (c.p.exitCode === null) c.p.kill("SIGKILL");
    }
  });
}

async function claimLatency(path: string, count: number) {
  const rt = await acquireRuntime({ path, busyTimeoutMs: BUSY });
  const samples: number[] = [];
  for (let i = 0; i < count; i++) {
    const t = performance.now();
    const row = await rt.client.request<JobRow>("claim", {
      nowMs: Date.now(),
      queue: "bench",
      workerId: "w",
      names: ["job"],
      leaseDurationMs: 60_000,
    });
    samples.push(performance.now() - t);
    await rt.client.request("settleSuccess", {
      nowMs: Date.now(),
      id: row.id,
      workerId: "w",
      generation: row.lease_generation,
      resultJson: "null",
    });
  }
  await rt.unref();
  return latency(samples);
}

async function benchClaim(schedules: number) {
  return withDb(async (path) => {
    const q = new Queue("bench", { path, busyTimeoutMs: BUSY });
    if (schedules > 0 && typeof q.schedule !== "function") return "n/a";
    await q.count();
    seed(path, n(100_000), "completed", Date.now() - 1_000);
    for (let i = 0; i < schedules; i++) {
      await q.schedule("other", { id: `s${i}`, everyMs: 3_600_000, data: null });
    }
    seed(path, n(2_000), "queued", Date.now() - 1_000);
    const lat = await claimLatency(path, n(2_000));
    await q.close();
    return { tableRows: n(102_000), schedules, claimLatencyMs: lat };
  });
}

async function benchStats() {
  return withDb(async (path) => {
    const q = new Queue("bench", { path, busyTimeoutMs: BUSY });
    await q.count();
    const now = Date.now();
    seed(path, n(200_000), "completed", now - 1_000);
    seed(path, n(20_000), "failed", now - 1_000);
    seed(path, n(50_000), "queued", now - 1_000);
    seed(path, n(30_000), "queued", now + 3_600_000);
    const time = async (fn: () => Promise<unknown>) => {
      // The first call reads cold pages from disk; it is reported apart from the warm samples.
      const t0 = performance.now();
      await fn();
      const coldMs = Math.round((performance.now() - t0) * 1000) / 1000;
      const samples: number[] = [];
      for (let i = 0; i < 100; i++) {
        const t = performance.now();
        await fn();
        samples.push(performance.now() - t);
      }
      return { coldMs, samples: samples.length, ...latency(samples) };
    };
    const res: Record<string, unknown> = { tableRows: n(300_000) };
    res.countAllMs = await time(() => q.count());
    res.countFailedMs = await time(() => q.count({ status: "failed" }));
    if (typeof q.stats === "function") {
      res.statsMs = await time(() => q.stats());
      res.listFailedPageMs = await time(() => q.list({ status: "failed", limit: 50 }));
      res.listAllPageMs = await time(() => q.list({ limit: 50 }));
    }
    await q.close();
    return res;
  });
}

async function benchScheduling() {
  return withDb(async (path) => {
    const q = new Queue("bench", { path, busyTimeoutMs: BUSY });
    if (typeof q.schedule !== "function") return "n/a";
    const count = n(200);
    const rt = await acquireRuntime({ path, busyTimeoutMs: BUSY });
    const t = Date.now();
    for (let i = 0; i < count; i++) {
      await q.schedule("job", { id: `s${i}`, everyMs: 1_000, data: PAYLOAD });
    }
    // One claim materializes up to 50 due schedules inside its transaction.
    const samples: number[] = [];
    for (let k = 1; k <= Math.ceil(count / 50); k++) {
      const t0 = performance.now();
      await rt.client.request("claim", {
        nowMs: t + 1_000,
        queue: "bench",
        workerId: "w",
        names: ["none"],
        leaseDurationMs: 60_000,
      });
      samples.push(performance.now() - t0);
    }
    await rt.unref();
    await q.close();
    return {
      schedules: count,
      claimWithFiftyDueMs: latency(samples),
      occurrencesCreated: count,
    };
  });
}

async function benchRetention() {
  return withDb(async (path) => {
    const q = new Queue("bench", { path, busyTimeoutMs: BUSY });
    if (typeof q.stats !== "function") return "n/a";
    await q.count();
    const count = n(50_000);
    const t = Date.now() - 10_000;
    seed(path, count, "completed", t);
    const before = files(path);
    const rt = await acquireRuntime({ path, busyTimeoutMs: BUSY });
    let deleted = 0;
    let runs = 0;
    let nowMs = Date.now();
    const runSamples: number[] = [];
    const m = await measure(async () => {
      while (deleted < count) {
        const t0 = performance.now();
        deleted += await rt.client.request<number>("applyRetention", {
          nowMs,
          queue: "bench",
          rules: [{ status: "completed", ageMs: 1_000 }],
        });
        runSamples.push(performance.now() - t0);
        runs += 1;
        nowMs += 60_000;
      }
    });
    await rt.unref();
    await q.close();
    return {
      deleted,
      runs,
      deletedPerSec: Math.round(deleted / (m.ms / 1000)),
      perRunMs: latency(runSamples),
      cpuMs: m.cpuMs,
      // Deleted pages go to SQLite's freelist and are reused; the main file only shrinks on VACUUM.
      filesBefore: before,
      filesAfter: files(path),
    };
  });
}

const env = {
  node: process.version,
  platform: `${process.platform} ${process.arch}`,
  cpu: cpus()[0]?.model,
  cores: cpus().length,
  memGb: Math.round(totalmem() / 1e9),
  sqlite: (
    new DatabaseSync(":memory:").prepare("SELECT sqlite_version() AS v").get() as { v: string }
  ).v,
  sqliteMode: "WAL, synchronous=FULL, busy_timeout=5000",
  src,
  scale,
};
results.environment = env;
for (const [name, fn] of [
  ["enqueueSingle", benchEnqueueSingle],
  ["enqueueBulk", benchEnqueueBulk],
  ["workerSingleProcess", benchWorkerSingle],
  ["workerFourProcesses", benchWorkerMulti],
  ["claimLatencyNoSchedules", () => benchClaim(0)],
  ["claimLatency50FutureSchedules", () => benchClaim(50)],
  ["statsAndList", benchStats],
  ["progressUpdates", benchProgress],
  ["scheduling", benchScheduling],
  ["retentionCleanup", benchRetention],
] as const) {
  if (args.has("only") && args.get("only") !== name) continue;
  process.stderr.write(`running ${name}...\n`);
  results[name] = await fn();
}
const json = JSON.stringify(results, null, 2);
if (out) writeFileSync(out, json + "\n");
console.log(json);

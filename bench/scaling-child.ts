// One worker process of a scaling scenario. Opens storage, prints "ready", waits for "go",
// processes until every seeded job is terminal (or the deadline passes), then reports.
import { appendFileSync } from "node:fs";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { Worker as Thread } from "node:worker_threads";

const [src, path, total, concurrency, execLog, deadlineMs] = process.argv.slice(2);
if (!src || !path || !total || !concurrency || !execLog || !deadlineMs) {
  throw new Error(
    "usage: scaling-child.ts <src> <db> <total> <concurrency> <execLog> <deadlineMs>",
  );
}
// Route this process's storage through the diagnostic worker.
(globalThis as Record<symbol, unknown>)[Symbol.for("durableq.spawnStorageWorker")] = () =>
  new Thread(new URL("./storage-worker-diag.ts", import.meta.url), {
    workerData: { src },
    env: process.env,
    execArgv: [],
  });
const { Queue } = (await import(
  pathToFileURL(join(src, "queue.ts")).href
)) as typeof import("../src/queue.ts");
const { Worker } = (await import(
  pathToFileURL(join(src, "worker.ts")).href
)) as typeof import("../src/worker.ts");
const { acquireRuntime } = (await import(
  pathToFileURL(join(src, "storage/runtime-registry.ts")).href
)) as typeof import("../src/storage/runtime-registry.ts");

const q = new Queue("bench", { path, busyTimeoutMs: 5_000 });
await q.count();
const executed: string[] = [];
const workerErrors: Record<string, number> = {};
const worker = new Worker(
  q,
  {
    job: async (job) => {
      executed.push(job.id);
      return null;
    },
  },
  { concurrency: Number(concurrency), pollIntervalMs: 5 },
);
worker.on("error", (err: { name?: string; code?: string }) => {
  const key = `${err.name ?? "Error"}:${err.code ?? "?"}`;
  workerErrors[key] = (workerErrors[key] ?? 0) + 1;
});
process.stdout.write("ready\n");
const lines = createInterface({ input: process.stdin });
for await (const line of lines) if (line === "go") break;
lines.close();

const cpu0 = process.cpuUsage();
const deadline = Date.now() + Number(deadlineMs);
let timedOut = false;
await worker.start();
for (;;) {
  await new Promise((r) => setTimeout(r, 50));
  const terminal = (await q.count({ status: "completed" })) + (await q.count({ status: "failed" }));
  if (terminal >= Number(total)) break;
  if (Date.now() > deadline) {
    timedOut = true;
    break;
  }
}
await worker.close({ timeoutMs: 5_000 });
const rt = await acquireRuntime({ path, busyTimeoutMs: 5_000 });
const diag = await rt.client.request("diag.stats", {});
await rt.unref();
await q.close();
const cpu = process.cpuUsage(cpu0);
appendFileSync(execLog, executed.map((id) => id + "\n").join(""));
process.stdout.write(
  JSON.stringify({
    timedOut,
    executions: executed.length,
    cpuMs: Math.round((cpu.user + cpu.system) / 1000),
    workerErrors,
    diag,
  }) + "\n",
);

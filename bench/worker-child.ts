// One worker process for the multi-process benchmark. Prints "ready" once its storage is open,
// waits for "go" on stdin (so no process starts working while another is still initializing),
// then drains jobs until all of them are completed.
import { join } from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";

const [src, path, total] = process.argv.slice(2);
if (!src || !path || !total) throw new Error("usage: worker-child.ts <src> <db> <total>");
const { Queue } = (await import(
  pathToFileURL(join(src, "queue.ts")).href
)) as typeof import("../src/queue.ts");
const { Worker } = (await import(
  pathToFileURL(join(src, "worker.ts")).href
)) as typeof import("../src/worker.ts");

const q = new Queue("bench", { path, busyTimeoutMs: 5_000 });
await q.count();
let errors = 0;
const worker = new Worker(q, { job: async () => null }, { concurrency: 8, pollIntervalMs: 5 });
worker.on("error", () => (errors += 1));
process.stdout.write("ready\n");
const lines = createInterface({ input: process.stdin });
for await (const line of lines) if (line === "go") break;
lines.close();

await worker.start();
for (;;) {
  await new Promise((r) => setTimeout(r, 50));
  if ((await q.count({ status: "completed" })) >= Number(total)) break;
}
await worker.close({ timeoutMs: 5_000 });
await q.close();
process.stdout.write(JSON.stringify({ errors }) + "\n");

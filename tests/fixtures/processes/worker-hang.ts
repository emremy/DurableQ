import { writeFileSync } from "node:fs";
import { Queue } from "../../../src/queue.ts";
import { Worker } from "../../../src/worker.ts";

const path = process.argv[2];
const mode = process.argv[3] ?? "after-claim";
const marker = process.argv[4];
if (!path) throw new Error("db path required");
process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
const queue = new Queue("app", { path, busyTimeoutMs: 50 });
const worker = new Worker(
  queue,
  {
    n: async () => {
      process.stdout.write(JSON.stringify({ started: true, pid: process.pid }) + "\n");
      if (marker) writeFileSync(marker, JSON.stringify({ started: true, pid: process.pid }));
      if (mode === "hang") {
        await new Promise(() => {
          /* hang until kill */
        });
      }
      return { ok: true };
    },
  },
  { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 400, heartbeatIntervalMs: 50 },
);
await worker.start();
await new Promise(() => {
  /* keep alive */
});

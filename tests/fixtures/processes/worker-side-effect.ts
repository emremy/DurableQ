import { Queue } from "../../../src/queue.ts";
import { Worker } from "../../../src/worker.ts";

const path = process.argv[2];
const url = process.argv[3];
if (!path || !url) throw new Error("db path and url required");
process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
const queue = new Queue("app", { path, busyTimeoutMs: 50 });
const worker = new Worker(
  queue,
  {
    n: async (job) => {
      await fetch(url, { headers: { "idempotency-key": job.id } });
      await new Promise(() => {
        /* hang until SIGKILL; completion ACK never happens */
      });
      return { ok: true };
    },
  },
  { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 400, heartbeatIntervalMs: 50 },
);
await worker.start();
await new Promise(() => {
  /* keep alive */
});

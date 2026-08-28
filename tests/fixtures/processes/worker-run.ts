import { Queue } from "../../../src/queue.ts";
import { Worker } from "../../../src/worker.ts";

const path = process.argv[2];
const durationMs = Number(process.argv[3] ?? 6_000);
if (!path) throw new Error("db path required");
process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
const queue = new Queue("app", { path, busyTimeoutMs: 2_000 });
const worker = new Worker(
  queue,
  {
    n: async (job) => ({ ok: true, id: job.id }),
    email: async (job) => ({ sent: true, id: job.id }),
    report: async (job) => ({ path: `/tmp/${job.id}` }),
    fail: async () => {
      throw new Error("retryable");
    },
  },
  {
    concurrency: 2,
    pollIntervalMs: 20,
    leaseDurationMs: 5_000,
    heartbeatIntervalMs: 50,
  },
);
await worker.start();
await new Promise((r) => setTimeout(r, durationMs));
await worker.close({ timeoutMs: 2_000 });
await queue.close();

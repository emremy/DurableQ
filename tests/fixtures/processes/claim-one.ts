import { ensureTestWorker } from "../../support/install-test-worker.ts";
import { Queue } from "../../../src/queue.ts";
import { acquireRuntime } from "../../../src/storage/runtime-registry.ts";

const path = process.argv[2];
const workerId = process.argv[3] ?? `w-${process.pid}`;
if (!path) throw new Error("db path required");
ensureTestWorker();
const queue = new Queue("app", { path, busyTimeoutMs: 2000 });
try {
  await queue.count();
  const rt = await acquireRuntime({ path, busyTimeoutMs: 2000 });
  try {
    const row = await rt.client.request("claim", {
      nowMs: Date.now(),
      queue: "app",
      workerId,
      names: ["n"],
      leaseDurationMs: 30_000,
    });
    if (row)
      process.stdout.write(
        JSON.stringify({ claimed: true, id: (row as { id: string }).id }) + "\n",
      );
    else process.stdout.write(JSON.stringify({ claimed: false }) + "\n");
  } finally {
    await rt.unref();
  }
} finally {
  await queue.close();
}

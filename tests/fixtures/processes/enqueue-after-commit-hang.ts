import { ensureTestWorker } from "../../support/install-test-worker.ts";
import { Queue } from "../../../src/queue.ts";
import { acquireRuntime } from "../../../src/storage/runtime-registry.ts";

const path = process.argv[2];
const dedupeKey = process.argv[3] ?? "k";
if (!path) throw new Error("db path required");
ensureTestWorker();
const queue = new Queue("app", { path, busyTimeoutMs: 2000 });
await queue.count();
const rt = await acquireRuntime({ path, busyTimeoutMs: 2000 });
try {
  await rt.client.request("test.configureFault", {
    point: "enqueue:after-commit",
    action: "hang",
  });
  process.stdout.write(JSON.stringify({ hanging: true, pid: process.pid }) + "\n");
  await queue.add("n", { v: 1 }, { dedupeKey });
  await new Promise(() => {
    /* unreachable: storage worker hangs after COMMIT */
  });
} finally {
  await rt.unref();
  await queue.close();
}

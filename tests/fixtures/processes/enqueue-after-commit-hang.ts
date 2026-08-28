import { Queue } from "../../../src/queue.ts";

const path = process.argv[2];
const dedupeKey = process.argv[3] ?? "k";
if (!path) throw new Error("db path required");
process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
const queue = new Queue("app", { path, busyTimeoutMs: 2000 });
const client = await queue.storage();
await client.request("test.configureFault", { point: "enqueue:after-commit", action: "hang" });
process.stdout.write(JSON.stringify({ hanging: true, pid: process.pid }) + "\n");
await queue.add("n", { v: 1 }, { dedupeKey });
await new Promise(() => {
  /* unreachable: storage worker hangs after COMMIT */
});

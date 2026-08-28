import { Queue } from "../../../src/queue.ts";

const path = process.argv[2];
const workerId = process.argv[3] ?? `w-${process.pid}`;
if (!path) throw new Error("db path required");
process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
const queue = new Queue("app", { path, busyTimeoutMs: 2000 });
try {
  const client = await queue.storage();
  const row = await client.request("claim", {
    nowMs: Date.now(),
    queue: "app",
    workerId,
    names: ["n"],
    leaseDurationMs: 30_000,
  });
  if (row)
    process.stdout.write(JSON.stringify({ claimed: true, id: (row as { id: string }).id }) + "\n");
  else process.stdout.write(JSON.stringify({ claimed: false }) + "\n");
} finally {
  await queue.close();
}

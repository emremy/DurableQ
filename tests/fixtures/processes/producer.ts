import { Queue } from "../../../src/queue.ts";

const path = process.argv[2];
const count = Number(process.argv[3] ?? 10);
const prefix = process.argv[4] ?? "p";
if (!path) throw new Error("db path required");
process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
const queue = new Queue("app", { path, busyTimeoutMs: 2_000 });
const ids: string[] = [];
for (let i = 0; i < count; i++) {
  const name = i % 3 === 0 ? "email" : i % 3 === 1 ? "report" : "n";
  const { job } = await queue.add(
    name,
    { i, prefix },
    {
      priority: i % 5,
      delayMs: i % 7 === 0 ? 30 : 0,
      attempts: 3,
      backoff: { type: "none" },
      dedupeKey: `${prefix}:${i}`,
    },
  );
  ids.push(job.id);
}
process.stdout.write(JSON.stringify({ ids, count: ids.length }) + "\n");
await queue.close();

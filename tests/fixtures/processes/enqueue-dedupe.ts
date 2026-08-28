import { Queue } from "../../../src/queue.ts";

const path = process.argv[2];
if (!path) throw new Error("db path required");
process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
const queue = new Queue("app", { path, busyTimeoutMs: 2000 });
const { job } = await queue.add("n", { v: 1 }, { dedupeKey: process.argv[3] ?? "k" });
process.stdout.write(JSON.stringify({ id: job.id }) + "\n");
await queue.close();

import { Queue } from "../../../src/queue.ts";

const path = process.argv[2];
if (!path) throw new Error("db path required");
process.env.DURABLEQ_ENABLE_TEST_HOOKS = "1";
const queue = new Queue("app", { path, busyTimeoutMs: 5000 });
await queue.count();
await queue.close();

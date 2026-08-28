import { Queue } from "../../../src/queue.ts";
import { acquireRuntime } from "../../../src/storage/runtime-registry.ts";

const path = process.argv[2];
if (!path) throw new Error("db path required");
delete process.env.DURABLEQ_ENABLE_TEST_HOOKS;
const queue = new Queue("app", { path, busyTimeoutMs: 50 });
try {
  await queue.count();
  const rt = await acquireRuntime({ path, busyTimeoutMs: 50 });
  try {
    await rt.client.request("test.configureFault", {
      point: "enqueue:before-begin",
      action: "throw",
    });
    process.stdout.write(JSON.stringify({ ok: false, reason: "hooks-allowed" }) + "\n");
  } catch (err) {
    process.stdout.write(
      JSON.stringify({
        ok: true,
        name: err instanceof Error ? err.name : "unknown",
        message: err instanceof Error ? err.message : String(err),
      }) + "\n",
    );
  } finally {
    await rt.unref();
  }
} finally {
  await queue.close();
}

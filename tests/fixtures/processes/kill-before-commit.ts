import { ensureTestWorker } from "../../support/install-test-worker.ts";
import { Queue } from "../../../src/queue.ts";
import { acquireRuntime } from "../../../src/storage/runtime-registry.ts";

const path = process.argv[2];
const mode = process.argv[3];
const gateDir = process.argv[4];
if (!path || !mode || !gateDir) {
  throw new Error("usage: kill-before-commit.ts <db> <enqueue|claim|settle> <gateDir>");
}

ensureTestWorker();
const queue = new Queue("app", { path, busyTimeoutMs: 50 });
await queue.count();
const rt = await acquireRuntime({ path, busyTimeoutMs: 50 });

try {
  if (mode === "enqueue") {
    await rt.client.request("test.configureFault", {
      point: "enqueue:before-commit",
      action: "hang",
      gateDir,
    });
    await queue.add("n", { v: 1 });
  } else if (mode === "claim") {
    await queue.add("n", { v: 1 });
    await rt.client.request("test.configureFault", {
      point: "claim:before-commit",
      action: "hang",
      gateDir,
    });
    await rt.client.request("claim", {
      nowMs: Date.now(),
      queue: "app",
      workerId: "w1",
      names: ["n"],
      leaseDurationMs: 30_000,
    });
  } else if (mode === "settle") {
    await queue.add("n", { v: 1 });
    const claimed = (await rt.client.request("claim", {
      nowMs: Date.now(),
      queue: "app",
      workerId: "w1",
      names: ["n"],
      leaseDurationMs: 30_000,
    })) as { id: string; lease_generation: number };
    await rt.client.request("test.configureFault", {
      point: "settleSuccess:before-commit",
      action: "hang",
      gateDir,
    });
    await rt.client.request("settleSuccess", {
      nowMs: Date.now(),
      id: claimed.id,
      workerId: "w1",
      generation: claimed.lease_generation,
      resultJson: "{}",
    });
  } else {
    throw new Error(`unknown mode ${mode}`);
  }
} finally {
  await rt.unref().catch(() => undefined);
  await queue.close().catch(() => undefined);
}

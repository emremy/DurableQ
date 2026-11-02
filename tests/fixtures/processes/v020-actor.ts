// One process acting on a shared database for the v0.2.0 multi-process and crash tests.
import { writeFileSync } from "node:fs";
import { ensureTestWorker } from "../../support/install-test-worker.ts";
import { Queue } from "../../../src/queue.ts";
import { Worker } from "../../../src/worker.ts";
import { acquireRuntime } from "../../../src/storage/runtime-registry.ts";
import type { JobRow } from "../../../src/storage/protocol.ts";

const [path, mode, arg = "", gateDir = ""] = process.argv.slice(2);
if (!path || !mode) throw new Error("usage: v020-actor.ts <db> <mode> [arg] [gateDir]");

ensureTestWorker();
const queue = new Queue("app", { path, busyTimeoutMs: 5_000 });
await queue.count();
const rt = await acquireRuntime({ path, busyTimeoutMs: 5_000 });
const fault = (point: string, action: "hang" | "none" = "hang") =>
  rt.client.request("test.configureFault", { point, action, gateDir });

try {
  switch (mode) {
    case "schedule-race": {
      // arg: JSON array of timestamps; claim + settle "tick" at each.
      const times = JSON.parse(arg) as number[];
      const ids: string[] = [];
      for (const nowMs of times) {
        const row = await rt.client.request<JobRow | null>("claim", {
          nowMs,
          queue: "app",
          workerId: `w-${process.pid}`,
          names: ["tick"],
          leaseDurationMs: 60 * 60 * 1000,
        });
        if (!row) continue;
        ids.push(row.id);
        await rt.client.request("settleSuccess", {
          nowMs,
          id: row.id,
          workerId: `w-${process.pid}`,
          generation: row.lease_generation,
          resultJson: "null",
        });
      }
      process.stdout.write(JSON.stringify(ids) + "\n");
      break;
    }
    case "add-many": {
      // arg: "<offset>"; 10 batches of 100, keys overlap with other processes.
      const offset = Number(arg);
      for (let b = 0; b < 10; b++) {
        await queue.addMany(
          Array.from({ length: 100 }, (_, i) => {
            const k = offset + b * 100 + i;
            return { name: "n", data: { k }, options: { dedupeKey: `k-${k % 1500}` } };
          }),
        );
      }
      break;
    }
    case "pause":
      await queue.pause();
      break;
    case "claim-loop": {
      // Claims until arg ms elapse; prints how many jobs it claimed.
      let claimed = 0;
      const until = Date.now() + Number(arg);
      while (Date.now() < until) {
        const row = await rt.client.request<JobRow | null>("claim", {
          nowMs: Date.now(),
          queue: "app",
          workerId: `w-${process.pid}`,
          names: ["n"],
          leaseDurationMs: 60_000,
        });
        if (row) claimed += 1;
      }
      process.stdout.write(JSON.stringify({ claimed }) + "\n");
      break;
    }
    case "retention": {
      const deleted = await rt.client.request<number>("applyRetention", {
        nowMs: Number(arg),
        queue: "app",
        rules: [{ status: "completed", ageMs: 1 }],
      });
      process.stdout.write(JSON.stringify({ deleted }) + "\n");
      break;
    }
    case "hang-enqueueMany":
      await fault("enqueue:before-commit");
      await queue.addMany(Array.from({ length: 200 }, (_, i) => ({ name: "n", data: { i } })));
      break;
    case "hang-schedule":
      await fault("claim:before-commit");
      await rt.client.request("claim", {
        nowMs: Number(arg),
        queue: "app",
        workerId: "w-crash",
        names: ["tick"],
        leaseDurationMs: 60_000,
      });
      break;
    case "hang-retention":
      await fault("retention:before-commit");
      await rt.client.request("applyRetention", {
        nowMs: Number(arg),
        queue: "app",
        rules: [{ status: "completed", ageMs: 1 }],
      });
      break;
    case "progress": {
      // arg: "before" hangs inside the progress transaction; "after" hangs once it committed.
      const worker = new Worker(
        queue,
        {
          n: async (_job, ctx) => {
            await ctx.updateProgress(42);
            if (arg === "before") {
              await fault("progress:before-commit");
              await ctx.updateProgress(77);
            }
            writeFileSync(`${gateDir}/paused`, "committed 42");
            await new Promise(() => undefined);
            return null;
          },
        },
        { pollIntervalMs: 10, leaseDurationMs: 30_000, heartbeatIntervalMs: 100 },
      );
      await worker.start();
      await new Promise(() => undefined);
      break;
    }
    default:
      throw new Error(`unknown mode ${mode}`);
  }
} finally {
  await rt.unref().catch(() => undefined);
  await queue.close().catch(() => undefined);
}

import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

test("soak mixed short workload", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    {
      ok: async () => ({ ok: true }),
      fail: async () => {
        throw new Error("x");
      },
    },
    { concurrency: 4, pollIntervalMs: 10, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  const extra = Number(process.env.DURABLEQ_SOAK_MS ?? 0);
  const okCount = extra > 0 ? 80 : 20;
  const failCount = extra > 0 ? 20 : 5;
  try {
    for (let i = 0; i < okCount; i++) await q.add("ok", { i });
    for (let i = 0; i < failCount; i++) await q.add("fail", { i }, { attempts: 1 });
    await worker.start();
    if (extra > 0) await new Promise((r) => setTimeout(r, extra));
    await eventually(
      async () => {
        assert.equal(
          (await q.count({ status: "completed" })) + (await q.count({ status: "failed" })),
          okCount + failCount,
        );
      },
      { timeoutMs: extra > 0 ? extra + 10_000 : 5_000 },
    );
    assertDatabaseInvariants(tmp.path);
    const db = new DatabaseSync(tmp.path);
    try {
      const integrity = db.prepare("PRAGMA integrity_check").get() as { integrity_check: string };
      assert.equal(integrity.integrity_check, "ok");
    } finally {
      db.close();
    }
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

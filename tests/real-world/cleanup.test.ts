import assert from "node:assert/strict";
import test from "node:test";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { Worker } from "../../src/worker.ts";
import { eventually } from "../support/eventually.ts";

test("RW-006 clean removes terminal jobs and attempt rows", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    { n: async () => ({ ok: true }) },
    { concurrency: 2, pollIntervalMs: 15, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    for (let i = 0; i < 8; i++) await q.add("n", { i });
    await worker.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "completed" }), 8);
    });
    await worker.close({ timeoutMs: 500 });
    const deleted = await q.clean({ status: "completed", before: Date.now() + 1000 });
    assert.equal(deleted.deleted, 8);
    assert.equal(await q.count(), 0);
    assertDatabaseInvariants(tmp.path);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

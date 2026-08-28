import assert from "node:assert/strict";
import test from "node:test";
import { NonRetryableError } from "../../src/errors.ts";
import { Queue } from "../../src/queue.ts";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { tempDbPath } from "../support/temp-db.ts";

test("production storage worker serves public Queue and Worker APIs", async () => {
  const tmp = tempDbPath();
  const q = new Queue("app", { path: tmp.path, busyTimeoutMs: 50 });
  const worker = new Worker(
    q,
    {
      ok: async () => ({ done: true }),
      boom: async () => {
        throw new NonRetryableError("nope");
      },
    },
    { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    const added = await q.add("ok", { n: 1 });
    assert.equal(await q.count({ status: "queued" }), 1);
    await worker.start();
    await eventually(async () => {
      assert.equal((await q.get(added.job.id))?.status, "completed");
    });

    const failing = await q.add("boom", { n: 2 }, { attempts: 3 });
    await eventually(async () => {
      assert.equal((await q.get(failing.job.id))?.status, "failed");
    });

    await worker.close({ timeoutMs: 1_000 });

    const toCancel = await q.add("ok", { n: 3 });
    const cancel = await q.cancel(toCancel.job.id);
    assert.equal(cancel.cancelled, true);

    const retried = await q.retry(failing.job.id);
    assert.equal(retried.retried, true);
    if (!retried.retried) throw new Error("expected retry");
    assert.equal(retried.job.error, null);

    await q.clean({ status: "completed", before: Date.now() + 1_000 });
    await q.clean({ status: "cancelled", before: Date.now() + 1_000 });
    assertDatabaseInvariants(tmp.path);
  } finally {
    await worker.close({ timeoutMs: 200 }).catch(() => undefined);
    await q.close();
    tmp.cleanup();
  }
});

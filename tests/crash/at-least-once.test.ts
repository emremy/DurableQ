import assert from "node:assert/strict";
import test from "node:test";
import { createServer } from "node:http";
import { Worker } from "../../src/worker.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";

test("CR-004 handler can run more than once; idempotency key is stable", async () => {
  const tmp = tempDbPath();
  const seen: string[] = [];
  const server = createServer((req, res) => {
    seen.push(String(req.headers["idempotency-key"] ?? ""));
    if (seen.length < 2) {
      res.statusCode = 500;
      res.end("no");
      return;
    }
    res.end("ok");
  });
  await new Promise<void>((r) => server.listen(0, r));
  const port = (server.address() as { port: number }).port;
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    {
      hook: async (job, ctx) => {
        const res = await fetch(`http://127.0.0.1:${port}/`, {
          signal: ctx.signal,
          headers: { "idempotency-key": job.id },
        });
        if (!res.ok) throw new Error("retry");
        return { ok: true };
      },
    },
    { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    const { job } = await q.add("hook", { port }, { attempts: 5, backoff: { type: "none" } });
    await worker.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "completed" }), 1);
    });
    assert.ok(seen.length >= 2);
    assert.ok(seen.every((k) => k === job.id));
    await worker.close({ timeoutMs: 500 });
    assertDatabaseInvariants(tmp.path);
  } finally {
    server.close();
    await q.close();
    tmp.cleanup();
  }
});

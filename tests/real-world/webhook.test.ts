import assert from "node:assert/strict";
import test from "node:test";
import { createServer } from "node:http";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";

test("RW-001 webhook retries until 200", async () => {
  const tmp = tempDbPath();
  let hits = 0;
  const server = createServer((req, res) => {
    hits += 1;
    if (hits < 3) {
      res.statusCode = 500;
      res.end("no");
      return;
    }
    res.statusCode = 200;
    res.end("ok");
  });
  await new Promise<void>((r) => server.listen(0, r));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  const q = openQueue(tmp.path);
  const worker = new Worker(
    q,
    {
      webhook: async (job, ctx) => {
        const res = await fetch(`http://127.0.0.1:${port}/`, {
          signal: ctx.signal,
          headers: { "idempotency-key": job.id },
        });
        if (!res.ok) throw new Error(`status ${res.status}`);
        return { ok: true };
      },
    },
    { concurrency: 1, pollIntervalMs: 20, leaseDurationMs: 5_000, heartbeatIntervalMs: 50 },
  );
  try {
    await q.add("webhook", { url: "/" }, { attempts: 5, backoff: { type: "none" } });
    await worker.start();
    await eventually(async () => {
      assert.equal(await q.count({ status: "completed" }), 1);
      assert.equal(hits, 3);
    });
    assertDatabaseInvariants(tmp.path);
  } finally {
    await worker.close({ timeoutMs: 500 });
    await q.close();
    server.close();
    tmp.cleanup();
  }
});

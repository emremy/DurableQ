import assert from "node:assert/strict";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { createFrozenClock } from "../../src/clock.ts";
import {
  RETENTION_BATCH_SIZE,
  RETENTION_INTERVAL_MS,
  RETENTION_MAX_BATCHES,
} from "../../src/constants.ts";
import { DurableQValidationError } from "../../src/errors.ts";
import { Queue } from "../../src/queue.ts";
import type { JobRow } from "../../src/storage/protocol.ts";
import type { QueueOptions } from "../../src/types.ts";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { ensureTestWorker } from "../support/install-test-worker.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { configureFault, rawRequest } from "../support/test-control.ts";

const T0 = 1_800_000_000_000;
const DAY = 24 * 60 * 60 * 1000;
type Rules = { status: "completed" | "failed" | "cancelled"; ageMs: number }[];

function open(path: string, clock = createFrozenClock(T0), extra: Partial<QueueOptions> = {}) {
  ensureTestWorker();
  return new Queue("app", { path, busyTimeoutMs: 50, ...extra }, clock);
}

async function run(
  q: Queue,
  name: string,
  nowMs: number,
  outcome: "ok" | "fail" | "retry" | "hold",
) {
  const row = await rawRequest<JobRow>(q, "claim", {
    nowMs,
    queue: "app",
    workerId: "w",
    names: [name],
    leaseDurationMs: 365 * DAY,
  });
  const base = { nowMs, id: row.id, workerId: "w", generation: row.lease_generation };
  if (outcome === "ok") await rawRequest(q, "settleSuccess", { ...base, resultJson: "1" });
  if (outcome !== "ok" && outcome !== "hold") {
    await rawRequest(q, "settleFailure", {
      ...base,
      error: { name: "Error", message: "x" },
      nonRetryable: outcome === "fail",
    });
  }
  return row;
}

function retain(q: Queue, nowMs: number, rules: Rules): Promise<number> {
  return rawRequest<number>(q, "applyRetention", { nowMs, queue: "app", rules });
}

test("retention options are validated", () => {
  const bad: unknown[] = [
    { completed: { ageMs: 0 } },
    { failed: { ageMs: -5 } },
    { cancelled: {} },
    { queued: { ageMs: 1_000 } },
    { completed: { ageMs: 1.5 } },
    "7d",
  ];
  for (const retention of bad) {
    assert.throws(
      () => open("x.db", undefined, { retention } as Partial<QueueOptions>),
      DurableQValidationError,
      JSON.stringify(retention),
    );
  }
  const q = open("x.db", undefined, { retention: { completed: { ageMs: DAY } } });
  assert.deepEqual(q.options.retention, [{ status: "completed", ageMs: DAY }]);
  assert.deepEqual(open("x.db").options.retention, []);
});

test("retention is disabled by default", async () => {
  const tmp = tempDbPath();
  const clock = createFrozenClock(T0);
  const q = open(tmp.path, clock);
  const worker = new Worker(q, { other: async () => null }, { pollIntervalMs: 10 });
  try {
    await q.add("ok", {});
    await run(q, "ok", T0, "ok");
    clock.advance(10 * 365 * DAY);
    await worker.start();
    await new Promise((r) => setTimeout(r, 100));
    assert.equal(await q.count({ status: "completed" }), 1);
  } finally {
    await worker.close({ timeoutMs: 500 });
    await q.close();
    tmp.cleanup();
  }
});

test("workers expire completed, failed and cancelled jobs by age and protect the rest", async () => {
  const tmp = tempDbPath();
  const clock = createFrozenClock(T0);
  const q = open(tmp.path, clock, {
    retention: { completed: { ageMs: DAY }, failed: { ageMs: 3 * DAY }, cancelled: { ageMs: DAY } },
  });
  const worker = new Worker(q, { unrelated: async () => null }, { pollIntervalMs: 10 });
  const events: { deleted: number }[] = [];
  worker.on("retention", (e: { deleted: number }) => events.push(e));
  try {
    for (const n of ["ok", "fail", "retry", "hold", "queued"]) await q.add(n, {});
    const cancelled = await q.add("cancel", {});
    await q.cancel(cancelled.job.id);
    await run(q, "ok", T0, "ok");
    await run(q, "fail", T0, "fail");
    await run(q, "retry", T0, "retry");
    const held = await run(q, "hold", T0, "hold");
    await q.add("fresh", {});
    await run(q, "fresh", T0 + 2 * DAY - 1, "ok");

    clock.set(T0 + 2 * DAY);
    await worker.start();
    await eventually(() => assert.equal(events.length, 1));
    assert.equal(events[0]?.deleted, 2);
    const left = (await q.list({ limit: 100 })).jobs.map((j) => `${j.name}:${j.status}`).sort();
    assert.deepEqual(left, [
      "fail:failed",
      "fresh:completed",
      "hold:active",
      "queued:queued",
      "retry:queued",
    ]);
    assert.equal((await q.get(held.id))?.status, "active");
    assertDatabaseInvariants(tmp.path, T0);
  } finally {
    await worker.close({ timeoutMs: 500 });
    await q.close();
    tmp.cleanup();
  }
});

test("one run per interval per queue, deleting in bounded batches", async () => {
  const tmp = tempDbPath();
  const q = open(tmp.path);
  try {
    const total = RETENTION_BATCH_SIZE * 2 + 17;
    for (let i = 0; i < total; i += 1_000) {
      await q.addMany(
        Array.from({ length: Math.min(1_000, total - i) }, (_, k) => ({
          name: "c",
          data: { k: i + k },
        })),
      );
    }
    for (const j of (await q.list({ limit: 1_000 })).jobs) await q.cancel(j.id);
    for (const j of (await q.list({ status: "queued", limit: 1_000 })).jobs) await q.cancel(j.id);
    assert.equal(await q.count({ status: "cancelled" }), total);

    const rules: Rules = [{ status: "cancelled", ageMs: 1 }];
    const now = T0 + DAY;
    const [a, b] = await Promise.all([retain(q, now, rules), retain(q, now, rules)]);
    assert.deepEqual(
      [a, b].sort((x, y) => x - y),
      [0, total],
    );
    assert.equal(await retain(q, now + RETENTION_INTERVAL_MS - 1, rules), 0);
    assert.equal(await q.count(), 0);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("a failed cleanup transaction deletes nothing and the next run recovers", async () => {
  const tmp = tempDbPath();
  const q = open(tmp.path);
  try {
    await q.add("ok", {});
    await run(q, "ok", T0, "ok");
    const rules: Rules = [{ status: "completed", ageMs: 1 }];
    await configureFault(q, { point: "retention:before-commit", action: "throw" });
    await assert.rejects(() => retain(q, T0 + DAY, rules), /injected fault/);
    await configureFault(q, { point: "retention:before-commit", action: "none" });
    assert.equal(await q.count(), 1);
    assert.equal(await retain(q, T0 + DAY + RETENTION_INTERVAL_MS, rules), 1);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("cleanup frees dedupe keys and does not disturb schedules", async () => {
  const tmp = tempDbPath();
  const clock = createFrozenClock(T0);
  const q = open(tmp.path, clock);
  try {
    const first = await q.add("ok", { v: 1 }, { dedupeKey: "k" });
    await run(q, "ok", T0, "ok");
    await q.schedule("tick", { id: "s", everyMs: 1_000, data: null });
    const occ = await run(q, "tick", T0 + 1_000, "ok");
    assert.equal(await retain(q, T0 + DAY, [{ status: "completed", ageMs: 1 }]), 2);

    clock.set(T0 + DAY);
    const again = await q.add("ok", { v: 2 }, { dedupeKey: "k" });
    assert.equal(again.deduplicated, false);
    assert.notEqual(again.job.id, first.job.id);

    const s = await q.getSchedule("s");
    assert.equal(s?.lastJobId, occ.id);
    const next = await run(q, "tick", T0 + DAY + 500, "ok");
    assert.equal(next.name, "tick");
    assert.equal(next.dedupe_key, `durableq:schedule:s:${T0 + DAY}`);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("the retention slot blocks other runs for a full interval", async () => {
  const tmp = tempDbPath();
  const q = open(tmp.path);
  try {
    const rules: Rules = [{ status: "completed", ageMs: 1 }];
    await q.add("ok", {});
    await run(q, "ok", T0, "ok");
    assert.equal(await retain(q, T0 + DAY, rules), 1);
    await q.add("ok", {});
    await run(q, "ok", T0, "ok");
    assert.equal(await retain(q, T0 + DAY, rules), 0, "same instant");
    assert.equal(
      await retain(q, T0 + DAY + RETENTION_INTERVAL_MS - 1, rules),
      0,
      "within interval",
    );
    assert.equal(await retain(q, T0 + DAY + RETENTION_INTERVAL_MS, rules), 1, "next interval");
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("failed and cancelled rules use their own timestamps", async () => {
  const tmp = tempDbPath();
  const q = open(tmp.path);
  try {
    await q.add("fail", {});
    await run(q, "fail", T0, "fail");
    const c = await q.add("cancel", {});
    await q.cancel(c.job.id);
    await q.add("ok", {});
    await run(q, "ok", T0, "ok");
    const deleted = await retain(q, T0 + DAY, [
      { status: "failed", ageMs: 1 },
      { status: "cancelled", ageMs: 1 },
    ]);
    assert.equal(deleted, 2);
    assert.deepEqual(
      (await q.list()).jobs.map((j) => j.status),
      ["completed"],
    );
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("one run deletes at most RETENTION_MAX_BATCHES batches per status", async () => {
  const tmp = tempDbPath();
  const q = open(tmp.path);
  try {
    const cap = RETENTION_MAX_BATCHES * RETENTION_BATCH_SIZE;
    const total = cap + 250;
    await q.count();
    const raw = new DatabaseSync(tmp.path);
    raw.exec(`
      WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < ${total})
      INSERT INTO durableq_jobs (id, queue, name, payload_json, status, available_at,
        max_attempts, created_at, updated_at, completed_at)
      SELECT lower(hex(randomblob(16))), 'app', 'n', '{}', 'completed', ${T0}, 3, ${T0}, ${T0}, ${T0}
      FROM c;`);
    raw.close();
    const rules: Rules = [{ status: "completed", ageMs: 1 }];
    assert.equal(await retain(q, T0 + DAY, rules), cap);
    assert.equal(await retain(q, T0 + DAY + RETENTION_INTERVAL_MS, rules), 250);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

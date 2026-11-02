import assert from "node:assert/strict";
import test from "node:test";
import { createFrozenClock } from "../../src/clock.ts";
import { DurableQSerializationError, DurableQValidationError } from "../../src/errors.ts";
import type { JobRow } from "../../src/storage/protocol.ts";
import type { Queue } from "../../src/queue.ts";
import { Worker } from "../../src/worker.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { eventually } from "../support/eventually.ts";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { rawRequest } from "../support/test-control.ts";

const T0 = 1_800_000_000_000;
const EVERY = 60_000;

function claimAt(q: Queue, nowMs: number, names = ["tick"]): Promise<JobRow | null> {
  return rawRequest<JobRow | null>(q, "claim", {
    nowMs,
    queue: q.name,
    workerId: "w-test",
    names,
    leaseDurationMs: 24 * 60 * 60 * 1000,
  });
}

async function settle(q: Queue, row: JobRow, nowMs: number): Promise<void> {
  await rawRequest(q, "settleSuccess", {
    nowMs,
    id: row.id,
    workerId: "w-test",
    generation: row.lease_generation,
    resultJson: "null",
  });
}

test("schedule create, idempotent re-declare, update, get and list", async () => {
  const tmp = tempDbPath();
  const clock = createFrozenClock(T0);
  const q = openQueue(tmp.path, "app", clock);
  try {
    const created = await q.schedule("tick", { id: "s1", everyMs: EVERY, data: { n: 1 } });
    assert.equal(created.outcome, "created");
    assert.equal(created.schedule.nextRunAt, T0 + EVERY);
    assert.deepEqual(created.schedule.data, { n: 1 });
    assert.equal(created.schedule.attempts, 3);
    assert.equal(created.schedule.lastJobId, null);

    clock.advance(1_000);
    const same = await q.schedule("tick", { id: "s1", everyMs: EVERY, data: { n: 1 } });
    assert.equal(same.outcome, "unchanged");
    assert.deepEqual(same.schedule, created.schedule);

    const newData = await q.schedule("tick", {
      id: "s1",
      everyMs: EVERY,
      data: { n: 2 },
      priority: 7,
    });
    assert.equal(newData.outcome, "updated");
    assert.equal(newData.schedule.nextRunAt, T0 + EVERY, "same interval keeps the grid");
    assert.equal(newData.schedule.priority, 7);

    const newInterval = await q.schedule("tick", { id: "s1", everyMs: 2 * EVERY, data: { n: 2 } });
    assert.equal(newInterval.outcome, "updated");
    assert.equal(newInterval.schedule.nextRunAt, T0 + 1_000 + 2 * EVERY);

    await q.schedule("other", { id: "s0", everyMs: EVERY, data: null });
    assert.deepEqual(
      (await q.listSchedules()).map((s) => s.id),
      ["s0", "s1"],
    );
    assert.equal((await q.getSchedule("s1"))?.everyMs, 2 * EVERY);
    assert.equal(await q.getSchedule("missing"), null);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("invalid schedules are rejected before anything is stored", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  try {
    for (const everyMs of [0, -1, 999, 1.5, Number.NaN, 400 * 24 * 60 * 60 * 1000]) {
      await assert.rejects(
        () => q.schedule("tick", { id: "bad", everyMs, data: null }),
        DurableQValidationError,
        `everyMs=${everyMs}`,
      );
    }
    await assert.rejects(
      () => q.schedule("tick", { id: "", everyMs: EVERY, data: null }),
      DurableQValidationError,
    );
    await assert.rejects(
      () => q.schedule("tick", { id: "bad", everyMs: EVERY, data: null, attempts: 0 }),
      DurableQValidationError,
    );
    await assert.rejects(
      () =>
        q.schedule("tick", {
          id: "bad",
          everyMs: EVERY,
          data: { d: new Date() } as unknown as null,
        }),
      DurableQSerializationError,
    );
    assert.deepEqual(await q.listSchedules(), []);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("due schedule materializes exactly one occurrence per slot", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path, "app", createFrozenClock(T0));
  try {
    await q.schedule("tick", { id: "s1", everyMs: EVERY, data: { n: 1 }, priority: 3 });
    assert.equal(await claimAt(q, T0 + EVERY - 1), null);
    assert.equal(await q.count(), 0);

    const first = await claimAt(q, T0 + EVERY);
    assert.ok(first);
    assert.equal(first.priority, 3);
    assert.equal(first.dedupe_key, `durableq:schedule:s1:${T0 + EVERY}`);
    assert.equal(await claimAt(q, T0 + EVERY + 10), null);
    await settle(q, first, T0 + EVERY + 20);

    for (let i = 2; i <= 4; i++) {
      const row = await claimAt(q, T0 + i * EVERY + 5);
      assert.ok(row, `occurrence ${i}`);
      assert.equal(row.dedupe_key, `durableq:schedule:s1:${T0 + i * EVERY}`);
      await settle(q, row, T0 + i * EVERY + 6);
    }
    assert.equal(await q.count({ status: "completed" }), 4);
    const s = await q.getSchedule("s1");
    assert.equal(s?.nextRunAt, T0 + 5 * EVERY);
    assertDatabaseInvariants(tmp.path, T0 + 5 * EVERY);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("extended downtime produces one catch-up occurrence and realigns to the grid", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path, "app", createFrozenClock(T0));
  try {
    await q.schedule("tick", { id: "s1", everyMs: EVERY, data: null });
    const late = T0 + 1_000 * EVERY + 123;
    const row = await claimAt(q, late);
    assert.ok(row);
    assert.equal(await q.count(), 1);
    assert.equal((await q.getSchedule("s1"))?.nextRunAt, T0 + 1_001 * EVERY);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("no new occurrence while the previous one is still queued or active", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path, "app", createFrozenClock(T0));
  try {
    await q.schedule("tick", { id: "s1", everyMs: EVERY, data: null });
    // Claims for other names still drive materialization; the occurrence stays queued.
    for (let i = 1; i <= 5; i++) await claimAt(q, T0 + i * EVERY, ["unrelated"]);
    assert.equal(await q.count(), 1);
    const active = await claimAt(q, T0 + 6 * EVERY);
    assert.ok(active);
    assert.equal(await claimAt(q, T0 + 7 * EVERY), null);
    assert.equal(await q.count(), 1);
    await settle(q, active, T0 + 7 * EVERY + 1);
    assert.ok(await claimAt(q, T0 + 8 * EVERY));
    assert.equal(await q.count(), 2);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("removing a schedule stops future occurrences but keeps created jobs", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path, "app", createFrozenClock(T0));
  try {
    await q.schedule("tick", { id: "s1", everyMs: EVERY, data: null });
    await claimAt(q, T0 + EVERY, ["unrelated"]);
    assert.equal(await q.count({ status: "queued" }), 1);
    assert.deepEqual(await q.removeSchedule("s1"), { removed: true });
    assert.deepEqual(await q.removeSchedule("s1"), { removed: false });
    const job = await claimAt(q, T0 + 10 * EVERY);
    assert.ok(job, "already created occurrence still runs");
    await settle(q, job, T0 + 10 * EVERY + 1);
    assert.equal(await claimAt(q, T0 + 20 * EVERY), null);
    assert.equal(await q.count(), 1);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("schedules survive restart and are isolated per queue", async () => {
  const tmp = tempDbPath();
  try {
    const q1 = openQueue(tmp.path, "app", createFrozenClock(T0));
    await q1.schedule("tick", { id: "s1", everyMs: EVERY, data: { keep: true } });
    await q1.close();

    const other = openQueue(tmp.path, "other", createFrozenClock(T0));
    const q2 = openQueue(tmp.path, "app", createFrozenClock(T0));
    try {
      assert.equal(await other.getSchedule("s1"), null);
      assert.equal(await claimAt(other, T0 + EVERY), null);
      const s = await q2.getSchedule("s1");
      assert.deepEqual(s?.data, { keep: true });
      const row = await claimAt(q2, T0 + EVERY);
      assert.equal(row?.payload_json, '{"keep":true}');
    } finally {
      await other.close();
      await q2.close();
    }
  } finally {
    tmp.cleanup();
  }
});

test("a worker executes a schedule repeatedly over multiple intervals", async () => {
  const tmp = tempDbPath();
  const clock = createFrozenClock(Date.now());
  const q = openQueue(tmp.path, "app", clock);
  const runs: number[] = [];
  const worker = new Worker(
    q,
    {
      tick: async (job) => {
        runs.push((job.data as { n: number }).n);
        return null;
      },
    },
    { pollIntervalMs: 10, leaseDurationMs: 5_000, heartbeatIntervalMs: 100 },
  );
  try {
    await q.schedule("tick", { id: "s1", everyMs: 1_000, data: { n: 1 } });
    await worker.start();
    for (let i = 1; i <= 5; i++) {
      clock.advance(1_000);
      await eventually(() => assert.equal(runs.length, i));
    }
    assert.deepEqual(runs, [1, 1, 1, 1, 1]);
    assert.equal(await q.count({ status: "completed" }), 5);
  } finally {
    await worker.close({ timeoutMs: 1_000 });
    await q.close();
    tmp.cleanup();
  }
});

test("every schedule field round-trips and reaches the generated job", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path, "app", createFrozenClock(T0));
  try {
    const { schedule } = await q.schedule("tick", {
      id: "full",
      everyMs: EVERY,
      data: { x: 1 },
      attempts: 4,
      priority: 9,
      attemptTimeoutMs: 1_234,
      backoff: { type: "exponential", delayMs: 10, maxDelayMs: 500 },
    });
    const expected = {
      attempts: 4,
      priority: 9,
      attemptTimeoutMs: 1_234,
      backoff: { type: "exponential", delayMs: 10, maxDelayMs: 500 },
    };
    for (const view of [schedule, await q.getSchedule("full"), (await q.listSchedules())[0]]) {
      assert.deepEqual(
        {
          attempts: view?.attempts,
          priority: view?.priority,
          attemptTimeoutMs: view?.attemptTimeoutMs,
          backoff: view?.backoff,
        },
        expected,
      );
    }
    const row = await claimAt(q, T0 + EVERY);
    assert.equal(row?.max_attempts, 4);
    assert.equal(row?.priority, 9);
    assert.equal(row?.attempt_timeout_ms, 1_234);
    assert.equal(row?.backoff_type, "exponential");
    assert.equal(row?.backoff_max_delay_ms, 500);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

test("changing any single field of a schedule is detected as an update", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path, "app", createFrozenClock(T0));
  try {
    const base = {
      id: "s",
      everyMs: EVERY,
      data: { v: 1 } as { v: number },
      attempts: 3,
      priority: 0,
      attemptTimeoutMs: 1_000,
      backoff: { type: "exponential", delayMs: 10, maxDelayMs: 100 } as {
        type: "exponential";
        delayMs: number;
        maxDelayMs: number;
      },
    };
    await q.schedule("tick", base);
    const variants: [string, string, typeof base][] = [
      ["name", "tock", base],
      ["data", "tick", { ...base, data: { v: 2 } }],
      ["everyMs", "tick", { ...base, everyMs: 2 * EVERY }],
      ["attempts", "tick", { ...base, attempts: 4 }],
      ["priority", "tick", { ...base, priority: 1 }],
      ["attemptTimeoutMs", "tick", { ...base, attemptTimeoutMs: 2_000 }],
      ["backoff.type", "tick", { ...base, backoff: { type: "fixed", delayMs: 10 } } as never],
      ["backoff.delayMs", "tick", { ...base, backoff: { ...base.backoff, delayMs: 20 } }],
      ["backoff.maxDelayMs", "tick", { ...base, backoff: { ...base.backoff, maxDelayMs: 200 } }],
    ];
    for (const [field, name, opts] of variants) {
      assert.equal((await q.schedule(name, opts)).outcome, "updated", field);
      assert.equal((await q.schedule("tick", base)).outcome, "updated", `${field} reverted`);
      assert.equal((await q.schedule("tick", base)).outcome, "unchanged", `${field} stable`);
    }
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

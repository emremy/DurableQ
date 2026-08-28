import assert from "node:assert/strict";
import test from "node:test";
import { openQueue } from "../support/open-queue.ts";
import { tempDbPath } from "../support/temp-db.ts";
import { assertDatabaseInvariants } from "../support/assert-db-invariants.ts";
import { rawRequest, setStorageNow, tickLeases } from "../support/test-control.ts";
import type { JobRow } from "../../src/storage/protocol.ts";

type ModelJob = {
  id: string;
  status: string;
  attemptCount: number;
  generation: number;
};

test("DB-backed state machine enumerated sequences", async () => {
  const tmp = tempDbPath();
  const q = openQueue(tmp.path);
  const model = new Map<string, ModelJob>();
  const seed = 1;
  try {
    await setStorageNow(q, 10_000);
    const steps = [
      "add",
      "add",
      "claim",
      "heartbeat",
      "complete",
      "add",
      "claim",
      "fail",
      "claim",
      "complete",
      "add",
      "cancel",
      "add",
      "claim",
      "expire",
    ];
    for (const step of steps) {
      try {
        if (step === "add") {
          const { job } = await q.add("n", { seed, t: Date.now() });
          if (!model.has(job.id)) {
            model.set(job.id, { id: job.id, status: "queued", attemptCount: 0, generation: 0 });
          }
        } else if (step === "claim") {
          const row = await rawRequest<JobRow | null>(q, "claim", {
            nowMs: 10_000,
            queue: q.name,
            workerId: "w",
            names: ["n"],
            leaseDurationMs: 5_000,
          });
          if (row) {
            model.set(row.id, {
              id: row.id,
              status: "active",
              attemptCount: row.attempt_count,
              generation: row.lease_generation,
            });
          }
        } else if (step === "heartbeat") {
          const active = [...model.values()].find((j) => j.status === "active");
          if (active) {
            await rawRequest(q, "heartbeat", {
              nowMs: 10_000,
              id: active.id,
              workerId: "w",
              generation: active.generation,
              leaseDurationMs: 5_000,
            });
          }
        } else if (step === "complete") {
          const active = [...model.values()].find((j) => j.status === "active");
          if (active) {
            await rawRequest(q, "settleSuccess", {
              nowMs: 10_000,
              id: active.id,
              workerId: "w",
              generation: active.generation,
              resultJson: "{}",
            });
            active.status = "completed";
          }
        } else if (step === "fail") {
          const active = [...model.values()].find((j) => j.status === "active");
          if (active) {
            const row = await rawRequest<JobRow>(q, "settleFailure", {
              nowMs: 10_000,
              id: active.id,
              workerId: "w",
              generation: active.generation,
              error: { name: "Error", message: "x" },
              nonRetryable: false,
            });
            model.set(active.id, {
              id: active.id,
              status: row.status,
              attemptCount: row.attempt_count,
              generation: row.lease_generation,
            });
          }
        } else if (step === "cancel") {
          const queued = [...model.values()].find((j) => j.status === "queued");
          if (queued) {
            await q.cancel(queued.id);
            queued.status = "cancelled";
          }
        } else if (step === "expire") {
          const active = [...model.values()].find((j) => j.status === "active");
          if (active) {
            await setStorageNow(q, 20_000);
            await tickLeases(q);
            const got = await q.get(active.id);
            if (got) {
              active.status = got.status;
              active.attemptCount = got.attemptCount;
              active.generation = got.leaseGeneration;
            }
          }
        }
        for (const m of model.values()) {
          const got = await q.get(m.id);
          assert.ok(got, `missing ${m.id} seed=${seed}`);
          assert.equal(got.status, m.status, `status ${m.id} seed=${seed}`);
        }
        assertDatabaseInvariants(tmp.path, 20_000);
      } catch (err) {
        throw new Error(`seed=${seed} step=${step}: ${err instanceof Error ? err.message : err}`);
      }
    }
    await tickLeases(q);
    assertDatabaseInvariants(tmp.path, 20_000);
  } finally {
    await q.close();
    tmp.cleanup();
  }
});

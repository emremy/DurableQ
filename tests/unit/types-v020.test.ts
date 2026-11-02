import test from "node:test";
import assert from "node:assert/strict";
import type { Queue } from "../../src/queue.ts";
import type { Worker } from "../../src/worker.ts";
import type { AddManyItem, JobContext, QueueStats } from "../../src/types.ts";

type Jobs = {
  "email.send": { input: { to: string }; output: null };
  "report.build": { input: { id: number }; output: { url: string } };
};

// Compile-time checks only: `npm run typecheck` fails if any expectation breaks.
function typeChecks(q: Queue<Jobs>, ctx: JobContext): void {
  void q.addMany([
    { name: "email.send", data: { to: "a@example.com" } },
    { name: "report.build", data: { id: 1 }, options: { priority: 1 } },
  ]);
  // @ts-expect-error unknown job name
  void q.addMany([{ name: "nope", data: {} }]);
  // @ts-expect-error payload does not match the job name
  void q.addMany([{ name: "email.send", data: { id: 1 } }]);
  void q.schedule("report.build", { id: "r", everyMs: 1_000, data: { id: 2 } });
  // @ts-expect-error schedule payload must match the job name
  void q.schedule("report.build", { id: "r", everyMs: 1_000, data: { to: "x" } });
  // @ts-expect-error delayMs is not a schedule option
  void q.schedule("report.build", { id: "r", everyMs: 1_000, data: { id: 2 }, delayMs: 5 });
  // @ts-expect-error status filter must be a real status
  void q.list({ status: "running" });
  const item: AddManyItem<Jobs> = { name: "report.build", data: { id: 3 } };
  void item;
  const p: Promise<void> = ctx.updateProgress(10);
  void p;
  const stats: Promise<QueueStats> = q.stats();
  void stats;
}

function workerChecks(W: typeof Worker, q: Queue<Jobs>): void {
  new W(q, {
    "email.send": async (_job, ctx) => {
      await ctx.updateProgress(50);
      return null;
    },
    "report.build": async (job) => ({ url: `/r/${job.data.id}` }),
  });
}

test("v0.2.0 public types compile", () => {
  assert.equal(typeof typeChecks, "function");
  assert.equal(typeof workerChecks, "function");
});

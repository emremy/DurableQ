// Multi-process scaling benchmark with full accounting of every job.
//   node bench/scaling.ts [--src dir] [--jobs 20000] [--repeat 3] [--matrix fixed|growing|both]
//                         [--scenarios 1x8,4x2] [--out file.json]
// "fixed" keeps total concurrency at 8 (1x8, 2x4, 4x2, 8x1); "growing" uses 8 per process
// (1x8, 2x8, 4x8, 8x8). Throughput counts completed jobs over wall time from "go" until every
// process has seen all jobs terminal and shut down.
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { cpus, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { seed } from "./seed.ts";

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) {
  args.set(process.argv[i]!.replace(/^--/, ""), process.argv[i + 1] ?? "");
}
const src = resolve(args.get("src") ?? new URL("../src", import.meta.url).pathname);
const jobs = Number(args.get("jobs") ?? "20000");
const repeat = Number(args.get("repeat") ?? "3");
const deadlineMs = Number(args.get("deadline") ?? "900000");
const matrix = args.get("matrix") ?? "both";
const scenarios = (
  args.get("scenarios") ??
  [
    ...(matrix !== "growing" ? ["1x8", "2x4", "4x2", "8x1"] : []),
    ...(matrix !== "fixed" ? ["1x8", "2x8", "4x8", "8x8"] : []),
  ].join(",")
)
  .split(",")
  .filter((s, i, all) => all.indexOf(s) === i)
  .map((s) => s.split("x").map(Number) as [number, number]);

const { Queue } = (await import(
  pathToFileURL(join(src, "queue.ts")).href
)) as typeof import("../src/queue.ts");

type ChildReport = {
  timedOut: boolean;
  executions: number;
  cpuMs: number;
  workerErrors: Record<string, number>;
  diag: {
    ops: Record<
      string,
      { calls: number; latencyMs: Record<string, number>; errors: Record<string, number> }
    >;
    transactions: Record<string, Record<string, Record<string, number>>>;
  };
};

function spawnChild(argv: string[]) {
  const script = new URL("./scaling-child.ts", import.meta.url).pathname;
  const p = spawn(process.execPath, [script, ...argv], { stdio: ["pipe", "pipe", "inherit"] });
  let text = "";
  const closed = new Promise<number | null>((r) => p.on("close", r));
  const ready = new Promise<void>((ok, fail) => {
    p.stdout.on("data", (d) => {
      text += String(d);
      if (text.includes("ready\n")) ok();
    });
    void closed.then((code) => fail(new Error(`child exited (${code}) before ready`)));
  });
  const done = closed.then((code) => {
    if (code !== 0) throw new Error(`child exit ${code}`);
    return JSON.parse(text.trim().split("\n").at(-1)!) as ChildReport;
  });
  return { p, ready, done };
}

const add = (into: Record<string, number>, from: Record<string, number>) => {
  for (const [k, v] of Object.entries(from)) into[k] = (into[k] ?? 0) + v;
};

async function runScenario(procs: number, concurrency: number) {
  const dir = mkdtempSync(join(tmpdir(), "dq-scale-"));
  const path = join(dir, "bench.db");
  const execLog = join(dir, "executions.log");
  writeFileSync(execLog, "");
  const children: ReturnType<typeof spawnChild>[] = [];
  try {
    const q = new Queue("bench", { path, busyTimeoutMs: 5_000 });
    await q.count();
    await q.close();
    seed(path, jobs, "queued", Date.now() - 1_000);
    for (let i = 0; i < procs; i++) {
      children.push(
        spawnChild([src, path, String(jobs), String(concurrency), execLog, String(deadlineMs)]),
      );
    }
    await Promise.all(children.map((c) => c.ready));
    const t0 = performance.now();
    for (const c of children) c.p.stdin.write("go\n");
    const reports = await Promise.all(children.map((c) => c.done));
    const wallMs = performance.now() - t0;

    const db = new DatabaseSync(path, { timeout: 5_000 });
    const status = Object.fromEntries(
      (
        db.prepare("SELECT status, COUNT(*) AS c FROM durableq_jobs GROUP BY status").all() as {
          status: string;
          c: number;
        }[]
      ).map((r) => [r.status, Number(r.c)]),
    );
    const attempts = db
      .prepare("SELECT outcome, COUNT(*) AS c FROM durableq_attempts GROUP BY outcome")
      .all() as {
      outcome: string | null;
      c: number;
    }[];
    db.close();
    const executions = readFileSync(execLog, "utf8").split("\n").filter(Boolean);
    const unique = new Set(executions).size;

    const workerErrors: Record<string, number> = {};
    const storageErrors: Record<string, Record<string, number>> = {};
    const calls: Record<string, number> = {};
    const worst: Record<string, number> = {};
    for (const r of reports) {
      add(workerErrors, r.workerErrors);
      for (const [name, o] of Object.entries(r.diag.ops)) {
        calls[name] = (calls[name] ?? 0) + o.calls;
        if (Object.keys(o.errors).length) add((storageErrors[name] ??= {}), o.errors);
        worst[`${name}.p95`] = Math.max(worst[`${name}.p95`] ?? 0, o.latencyMs.p95 ?? 0);
      }
      for (const [name, t] of Object.entries(r.diag.transactions)) {
        worst[`${name}.lockWait.p95`] = Math.max(
          worst[`${name}.lockWait.p95`] ?? 0,
          t.lockWaitMs?.p95 ?? 0,
        );
        worst[`${name}.body.p95`] = Math.max(worst[`${name}.body.p95`] ?? 0, t.bodyMs?.p95 ?? 0);
      }
    }
    const completed = status.completed ?? 0;
    const size = (p: string) => (existsSync(p) ? statSync(p).size : 0);
    return {
      processes: procs,
      concurrencyPerProcess: concurrency,
      totalConcurrency: procs * concurrency,
      jobs,
      valid: completed === jobs && reports.every((r) => !r.timedOut),
      jobsPerSec: Math.round(completed / (wallMs / 1000)),
      wallMs: Math.round(wallMs),
      finalStatus: status,
      attemptOutcomes: Object.fromEntries(attempts.map((a) => [a.outcome ?? "open", Number(a.c)])),
      handlerExecutions: executions.length,
      duplicateExecutions: executions.length - unique,
      claimCalls: calls.claim ?? 0,
      emptyClaims: (calls.claim ?? 0) - attempts.reduce((t, a) => t + Number(a.c), 0),
      workerErrors,
      storageErrorsByOp: storageErrors,
      worstProcessP95Ms: worst,
      cpuMs: reports.reduce((t, r) => t + r.cpuMs, 0),
      dbBytes: size(path),
      walBytes: size(`${path}-wal`),
    };
  } finally {
    for (const c of children) if (c.p.exitCode === null) c.p.kill("SIGKILL");
    rmSync(dir, { recursive: true, force: true });
  }
}

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
const results = [];
for (const [procs, conc] of scenarios) {
  const runs = [];
  for (let r = 0; r < repeat; r++) {
    process.stderr.write(`${procs}x${conc} run ${r + 1}/${repeat}...\n`);
    runs.push(await runScenario(procs, conc));
  }
  const tput = runs.map((r) => r.jobsPerSec);
  results.push({
    scenario: `${procs}x${conc}`,
    medianJobsPerSec: median(tput),
    minJobsPerSec: Math.min(...tput),
    maxJobsPerSec: Math.max(...tput),
    allValid: runs.every((r) => r.valid),
    runs,
  });
  const last = results.at(-1)!;
  process.stderr.write(
    `  -> median ${last.medianJobsPerSec} jobs/s (${last.minJobsPerSec}-${last.maxJobsPerSec}), valid=${last.allValid}\n`,
  );
}
const out = {
  environment: {
    node: process.version,
    cpu: cpus()[0]?.model,
    cores: cpus().length,
    src,
    jobs,
    repeat,
  },
  results,
};
if (args.get("out")) writeFileSync(args.get("out")!, JSON.stringify(out, null, 2) + "\n");
console.log(JSON.stringify(out, null, 2));

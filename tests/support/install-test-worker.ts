import { Worker } from "node:worker_threads";

const SPAWN = Symbol.for("durableq.spawnStorageWorker");
let installed = false;

export function ensureTestWorker(): void {
  if (installed) return;
  installed = true;
  (globalThis as Record<symbol, unknown>)[SPAWN] = () =>
    new Worker(new URL("./storage-worker-hooks.ts", import.meta.url), {
      env: process.env,
      execArgv: [],
    });
}

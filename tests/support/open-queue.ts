import { ensureTestWorker } from "./install-test-worker.ts";
import { Queue } from "../../src/queue.ts";
import type { Clock } from "../../src/clock.ts";
import { systemClock } from "../../src/clock.ts";

export function openQueue(path: string, name = "app", clock: Clock = systemClock): Queue {
  ensureTestWorker();
  return new Queue(name, { path, busyTimeoutMs: 50 }, clock);
}

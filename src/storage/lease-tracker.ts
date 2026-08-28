import type { DatabaseSync } from "node:sqlite";
import { LEASE_TICK_MS } from "../constants.ts";
import type { TrackPayload } from "./protocol.ts";

export type LeaseLostHandler = (
  jobId: string,
  generation: number,
  reason: "timeout" | "heartbeatFailed",
) => void;

type Tracked = TrackPayload & { lastHeartbeatAt: number };

export class LeaseTracker {
  private readonly tracked = new Map<string, Tracked>();
  private timer: ReturnType<typeof setInterval> | undefined;
  private nowFn: () => number;
  private heartbeat: (
    nowMs: number,
    id: string,
    workerId: string,
    generation: number,
    leaseDurationMs: number,
  ) => boolean;
  private onLost: LeaseLostHandler;

  constructor(
    _db: DatabaseSync,
    nowFn: () => number,
    heartbeat: LeaseTracker["heartbeat"],
    onLost: LeaseLostHandler,
  ) {
    this.nowFn = nowFn;
    this.heartbeat = heartbeat;
    this.onLost = onLost;
  }

  setNowFn(fn: () => number): void {
    this.nowFn = fn;
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      this.tick();
    }, LEASE_TICK_MS);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    this.tracked.clear();
  }

  track(payload: TrackPayload): void {
    this.tracked.set(payload.jobId, { ...payload, lastHeartbeatAt: this.nowFn() });
  }

  untrack(jobId: string, generation?: number): void {
    const cur = this.tracked.get(jobId);
    if (!cur) return;
    if (generation !== undefined && cur.generation !== generation) return;
    this.tracked.delete(jobId);
  }

  tick(): void {
    const now = this.nowFn();
    for (const [jobId, item] of this.tracked) {
      if (item.attemptDeadlineMs !== null && now >= item.attemptDeadlineMs) {
        this.tracked.delete(jobId);
        this.onLost(jobId, item.generation, "timeout");
        continue;
      }
      if (now - item.lastHeartbeatAt >= item.heartbeatIntervalMs) {
        try {
          const ok = this.heartbeat(
            now,
            item.jobId,
            item.workerId,
            item.generation,
            item.leaseDurationMs,
          );
          if (!ok) {
            this.tracked.delete(jobId);
            this.onLost(jobId, item.generation, "heartbeatFailed");
            continue;
          }
          item.lastHeartbeatAt = now;
        } catch {
          // SQLITE_BUSY or transient storage errors: retry on a later tick
        }
      }
    }
  }
}

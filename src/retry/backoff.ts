import { DurableQValidationError } from "../errors.ts";
import type { BackoffConfig } from "../types.ts";

const SAFE = Number.MAX_SAFE_INTEGER;

export function delayAfterFailedAttempt(backoff: BackoffConfig, failedAttempt: number): number {
  if (!Number.isSafeInteger(failedAttempt) || failedAttempt < 1) {
    throw new DurableQValidationError("failedAttempt must be a safe integer >= 1");
  }
  if (backoff.type === "none") return 0;
  if (backoff.type === "fixed") {
    assertDelay(backoff.delayMs, "backoff.delayMs");
    return backoff.delayMs;
  }
  assertDelay(backoff.delayMs, "backoff.delayMs");
  assertDelay(backoff.maxDelayMs, "backoff.maxDelayMs");
  if (backoff.maxDelayMs < backoff.delayMs) {
    throw new DurableQValidationError("backoff.maxDelayMs must be >= backoff.delayMs");
  }
  // min(delayMs * 2^(n-1), maxDelayMs) with overflow protection
  const exp = failedAttempt - 1;
  if (exp <= 0) return Math.min(backoff.delayMs, backoff.maxDelayMs);
  if (backoff.delayMs === 0) return 0;
  const maxFactor = Math.floor(SAFE / backoff.delayMs);
  const factor = exp >= 31 ? Infinity : 2 ** exp;
  if (!Number.isFinite(factor) || factor > maxFactor) {
    return backoff.maxDelayMs;
  }
  const raw = backoff.delayMs * factor;
  if (!Number.isSafeInteger(raw) || raw > backoff.maxDelayMs) return backoff.maxDelayMs;
  return raw;
}

export function assertDelay(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new DurableQValidationError(`${label} must be a safe integer >= 0`);
  }
}

export function assertBackoff(backoff: BackoffConfig): void {
  if (backoff.type === "none") return;
  if (backoff.type === "fixed") {
    assertDelay(backoff.delayMs, "backoff.delayMs");
    return;
  }
  if (backoff.type === "exponential") {
    assertDelay(backoff.delayMs, "backoff.delayMs");
    assertDelay(backoff.maxDelayMs, "backoff.maxDelayMs");
    if (backoff.maxDelayMs < backoff.delayMs) {
      throw new DurableQValidationError("backoff.maxDelayMs must be >= backoff.delayMs");
    }
    return;
  }
  throw new DurableQValidationError("invalid backoff type");
}

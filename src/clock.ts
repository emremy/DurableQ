export type Clock = {
  now(): number;
};

export const systemClock: Clock = {
  now: () => Date.now(),
};

export function createFrozenClock(
  start: number,
): Clock & { set(now: number): void; advance(ms: number): void } {
  let current = start;
  return {
    now: () => current,
    set(now: number) {
      current = now;
    },
    advance(ms: number) {
      current += ms;
    },
  };
}

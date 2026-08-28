export async function eventually(
  fn: () => Promise<void> | void,
  opts?: { timeoutMs?: number; intervalMs?: number },
): Promise<void> {
  const timeoutMs = opts?.timeoutMs ?? 5_000;
  const intervalMs = opts?.intervalMs ?? 25;
  const start = Date.now();
  let last: unknown;
  for (;;) {
    try {
      await fn();
      return;
    } catch (err) {
      last = err;
      if (Date.now() - start >= timeoutMs) throw last;
      await new Promise((r) => setTimeout(r, intervalMs));
    }
  }
}

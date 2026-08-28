import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export function makeTempDir(prefix = "durableq-"): string {
  return mkdtempSync(join(tmpdir(), prefix));
}

export function tempDbPath(): { dir: string; path: string; cleanup: () => void } {
  const dir = makeTempDir();
  const path = join(dir, "durableq.db");
  return {
    dir,
    path,
    cleanup: () => {
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

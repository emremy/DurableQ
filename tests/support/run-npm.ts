import { spawnSync, type SpawnSyncReturns } from "node:child_process";

/** Windows cannot exec `npm` (a .cmd) without a shell; status is then null. */
export function runNpm(args: string[], cwd: string): SpawnSyncReturns<string> {
  return spawnSync("npm", args, {
    cwd,
    encoding: "utf8",
    shell: process.platform === "win32",
  });
}

export function npmFailed(result: SpawnSyncReturns<string>): string {
  const err = result.error ? String(result.error) : "";
  return `${err}\n${result.stderr ?? ""}\n${result.stdout ?? ""}`;
}

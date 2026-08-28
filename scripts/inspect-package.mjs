#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));

if (pkg.dependencies && Object.keys(pkg.dependencies).length > 0) {
  console.error("runtime dependencies must be empty, found:", pkg.dependencies);
  process.exit(1);
}

if (pkg.engines?.node !== ">=24.15.0") {
  console.error("engines.node must be >=24.15.0, found:", pkg.engines?.node);
  process.exit(1);
}

if (pkg.type !== "module") {
  console.error("package type must be module");
  process.exit(1);
}

const packed = spawnSync("npm", ["pack", "--dry-run", "--json", "--ignore-scripts"], {
  cwd: root,
  encoding: "utf8",
});
if (packed.status !== 0) {
  console.error(packed.stderr);
  process.exit(packed.status ?? 1);
}

const data = JSON.parse(packed.stdout);
const files = (data[0]?.files ?? data.files ?? []).map((f) => f.path ?? f);
const forbidden = files.filter(
  (p) =>
    p.startsWith("tests/") ||
    p.startsWith("audits/") ||
    p.startsWith("coverage/") ||
    p.endsWith(".db") ||
    p.includes(".env"),
);
if (forbidden.length > 0) {
  console.error("tarball contains forbidden paths:", forbidden);
  process.exit(1);
}

const required = ["package.json", "LICENSE", "README.md"];
for (const r of required) {
  if (!files.includes(r) && !files.some((p) => p === r || p.endsWith("/" + r))) {
    console.error("tarball missing", r, "files=", files);
    process.exit(1);
  }
}

console.log("audit:package ok");
console.log("runtimeDependencies=0");
console.log("fileCount=" + files.length);

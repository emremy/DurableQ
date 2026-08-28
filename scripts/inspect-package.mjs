#!/usr/bin/env node
import { readFileSync, unlinkSync } from "node:fs";
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

const packed = spawnSync("npm", ["pack", "--ignore-scripts"], {
  cwd: root,
  encoding: "utf8",
});
if (packed.status !== 0) {
  console.error(packed.stderr);
  process.exit(packed.status ?? 1);
}

const tarball = packed.stdout
  .trim()
  .split("\n")
  .map((l) => l.trim())
  .filter((l) => l.endsWith(".tgz"))
  .at(-1);
if (!tarball) {
  console.error("npm pack did not print a tarball name");
  process.exit(1);
}

const listed = spawnSync("tar", ["tzf", tarball], { cwd: root, encoding: "utf8" });
if (listed.status !== 0) {
  console.error(listed.stderr);
  process.exit(listed.status ?? 1);
}

const files = listed.stdout
  .split("\n")
  .map((l) => l.trim())
  .filter(Boolean)
  .map((p) => p.replace(/^package\//, ""));

const forbiddenPaths = files.filter(
  (p) =>
    p.startsWith("tests/") ||
    p.startsWith("audits/") ||
    p.startsWith("coverage/") ||
    p.endsWith(".db") ||
    p.includes(".env"),
);
if (forbiddenPaths.length > 0) {
  console.error("tarball contains forbidden paths:", forbiddenPaths);
  process.exit(1);
}

const required = ["package.json", "LICENSE", "README.md"];
for (const r of required) {
  if (!files.includes(r)) {
    console.error("tarball missing", r, "files=", files);
    process.exit(1);
  }
}

const deny = [
  "test.configureFault",
  "test.setNow",
  "test.getNow",
  "test.tickLeases",
  "DURABLEQ_ENABLE_TEST_HOOKS",
  "enableTestHooks",
  "injected fault",
  "FaultSpec",
  "configureFault",
  "storage-worker-hooks",
];

const packedEntries = listed.stdout
  .split("\n")
  .map((l) => l.trim())
  .filter(Boolean);
for (const entry of packedEntries) {
  if (!/\.(js|mjs|cjs|d\.ts|map|json)$/i.test(entry)) continue;
  const extracted = spawnSync("tar", ["xOf", tarball, entry], { cwd: root, encoding: "utf8" });
  if (extracted.status !== 0) {
    console.error("failed to extract", entry, extracted.stderr);
    process.exit(1);
  }
  for (const token of deny) {
    if (extracted.stdout.includes(token)) {
      console.error(`tarball ${entry} contains forbidden identifier: ${token}`);
      process.exit(1);
    }
  }
}

try {
  unlinkSync(join(root, tarball));
} catch {
  // inspection artifact
}

console.log("audit:package ok");
console.log("runtimeDependencies=0");
console.log("fileCount=" + files.length);

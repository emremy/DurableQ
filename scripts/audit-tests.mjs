#!/usr/bin/env node
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const patterns = [
  { re: /\.only\s*\(/, label: ".only(" },
  { re: /\.skip\s*\(/, label: ".skip(" },
  { re: /\.todo\s*\(/, label: ".todo(" },
  { re: /@ts-ignore/, label: "@ts-ignore" },
  { re: /eslint-disable/, label: "eslint-disable" },
  { re: /istanbul ignore|c8 ignore|coverage ignore/i, label: "coverage ignore" },
];

const allowlist = new Set(["scripts/audit-tests.mjs"]);

/** @param {string} dir */
function walk(dir) {
  /** @type {string[]} */
  const out = [];
  for (const name of readdirSync(dir)) {
    if (name === "node_modules" || name === "dist" || name === ".git") continue;
    const p = join(dir, name);
    const st = statSync(p);
    if (st.isDirectory()) out.push(...walk(p));
    else if (/\.(ts|js|mjs|cjs)$/.test(name)) out.push(p);
  }
  return out;
}

let failed = 0;
for (const file of walk(root)) {
  const rel = relative(root, file);
  if (allowlist.has(rel)) continue;
  const text = readFileSync(file, "utf8");
  const lines = text.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? "";
    for (const p of patterns) {
      if (p.re.test(line)) {
        console.error(`${rel}:${i + 1}: forbidden pattern ${p.label}`);
        failed += 1;
      }
    }
  }
}

if (failed > 0) {
  console.error(`audit:tests failed (${failed} hits)`);
  process.exit(1);
}
console.log("audit:tests ok");

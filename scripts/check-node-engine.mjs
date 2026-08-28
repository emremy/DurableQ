#!/usr/bin/env node
const parts = process.versions.node.split(".");
const major = Number(parts[0]);
const minor = Number(parts[1]);
if (!Number.isFinite(major) || !Number.isFinite(minor)) {
  console.error("durableq: unable to parse Node.js version");
  process.exit(1);
}
if (major < 24 || (major === 24 && minor < 15)) {
  console.error(`durableq requires Node.js >=24.15.0 (current: ${process.versions.node})`);
  process.exit(1);
}

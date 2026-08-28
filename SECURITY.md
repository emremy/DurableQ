# Security

DurableQ is a local, single-machine job queue. It does not open network ports, send telemetry, or load SQLite extensions.

## Threat model (v0.1)

- Callers are trusted to choose a dedicated database file path.
- Job names, payloads, and dedupe keys are **bound parameters**, never interpolated into SQL.
- Payloads and results must be JSON-safe and are size-capped (1 MiB by default).
- Stored error text is truncated (64 KiB).
- `DatabaseSync` is opened with `defensive: true` and `allowExtension: false`.

## What DurableQ does not do

- Encrypt data at rest
- Authenticate callers
- Sandbox job handlers
- Protect a database file on a shared or network filesystem

## Reporting

Open a GitHub issue or contact the maintainers. Do not file crash-reproduction databases that contain production payloads.

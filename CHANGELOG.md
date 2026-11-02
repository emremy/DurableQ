# Changelog

## 0.2.0

Database schema version 2. A v0.1.0 database is upgraded automatically and in place on first open; see [Upgrading from v0.1.0](README.md#upgrading-from-v010). Downgrading is not supported.

### Added

- `queue.addMany(items)`: atomic bulk enqueue of up to 1,000 jobs in one transaction, with per-item deduplication and typed items.
- `queue.schedule(name, { id, everyMs, data, ... })`, `getSchedule`, `listSchedules`, `removeSchedule`: durable fixed-interval recurring jobs, created inside the claim transaction with at most one occurrence per slot across processes and one catch-up job after downtime.
- `queue.pause()`, `queue.resume()`, `queue.isPaused()`: persistent, cross-process pause, enforced in the claim transaction.
- `queue.stats()`: per-status counts plus `due`, `delayed`, `retrying`, `oldestQueuedAgeMs`, and `paused`.
- `queue.list({ status?, name?, limit?, cursor? })`: cursor-paginated job listing.
- `ctx.updateProgress(0–100)` and `JobView.progress`: durable, lease-fenced job progress.
- `QueueOptions.retention`: opt-in automatic deletion of old completed/failed/cancelled jobs, run by workers in bounded batches and coordinated across processes.
- Worker event `retention` (`{ deleted }`).
- Schema migration framework: versioned, transactional, safe under concurrent startup.

### Changed

- Opening an up-to-date database no longer takes the SQLite write lock, so a process starting next to busy workers no longer risks `SQLITE_BUSY` during initialization.
- A new attempt resets `progress` to 0 when it is claimed.
- The claim index now matches the claim order. In v0.1.0 every claim sorted all due jobs inside its write transaction, so claim cost grew with the backlog (about 8 ms per claim at 20,000 queued jobs, 120 ms at 80,000), throughput fell to about 70 jobs/s, and processes contending for that lock hit `SQLITE_BUSY`. Claims now take about 0.2 ms at any backlog size: about 2,000–2,700 jobs/s at 20,000 jobs, in every process layout we measured, with no `SQLITE_BUSY`.
- Completion and failure settlement retry `SQLITE_BUSY` up to 5 times with jittered backoff. A completion that still cannot be stored is no longer recorded as a handler failure (which previously caused the job to be retried and its handler to run again); the job stays active and is recovered when its lease expires.
- Storage statements are prepared once per connection.
- Upgrading requires stopping all v0.1.0 processes first; rolling upgrades are not supported.

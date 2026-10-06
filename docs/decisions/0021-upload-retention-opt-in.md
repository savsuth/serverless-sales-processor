# 0021. Uploads are kept forever unless a retention period is set

Status: accepted (2026-10-06)

## Context

Both buckets are versioned and nothing expired, so storage grows with
every file ([docs/costs.md](../costs.md)). Uploads are most of it.
Deleting them automatically is not reversible, and the project's rule
was never to delete data on its own.

## Decision

`upload_retention_days` (default `0`, keep forever) adds an S3
lifecycle rule to the input bucket only.
- **Current versions:** expire after that many days. S3 then adds a
  delete marker.
- **Noncurrent versions:** expire after the same number of days.
- **Overwritten uploads:** are covered by the noncurrent rule too, so
  every upload is kept at least the full period.
- **Reports and curated data:** never expire.

Values from 1 to 29 are refused. A message redriven from the
dead-letter queue (kept 14 days) must always find its upload.

## Consequences

- **Reprocessing:** an expired upload can no longer be reprocessed.
  `scripts/reprocess.py` checks the exact version first
  (`admin.upload_exists`) and skips expired ones rather than queueing a
  job that would fail on every attempt.
- **Removal timing:** S3 removes an upload between one and two periods
  after it was written.

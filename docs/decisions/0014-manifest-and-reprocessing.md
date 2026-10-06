# 0014. A manifest marks complete outputs; reprocessing re-queues the original event

Status: accepted (2026-10-05)

## Context

A job writes several objects (two reports and curated files) in
separate requests, so a reader could see a partial set. Separately,
after a validation rule changes, finished jobs need re-running on
purpose, which the claim logic otherwise prevents.

## Decision

- `reports/{job_id}/manifest.json` is written last and lists every
  object with its SHA-256 and size, plus status, counts, content
  fingerprint, and the schema name and version used. Its presence means
  the set is complete; `scripts/fetch_report.sh` verifies against it.
- Overwrites by a worker that lost its lease cannot happen here: the
  Lambda timeout (60 s) is shorter than the lease (120 s), so the
  worker is killed first ([0003](0003-timeout-lease-visibility-ordering.md)).
  Attempt-specific output prefixes were therefore not needed.
- Schemas have a `version`; every claim records it on the job.
- `scripts/reprocess.py` selects jobs by ID, status, or older schema
  version, marks each `failed`/`reprocess_requested` under a condition
  on its current status, and sends its original S3 event to the queue.
  The Lambda re-runs it like a retry and overwrites the same keys.

## Consequences

- Nothing is counted twice after a reprocess: report, curated and
  manifest keys are deterministic per job.
- Listing by status uses a new index (`status-created_at-index`);
  content fingerprint items have no status and stay out of it.

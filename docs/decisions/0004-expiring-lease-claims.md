# 0004. Jobs are claimed with expiring leases; every later write must hold the lease

Status: accepted (original build; recorded 2026-10-04)

## Context

With at-least-once delivery ([0001](0001-deterministic-job-ids.md)),
two invocations can work on the same job at the same time, and an
invocation can die after doing part of the work. The pipeline needs
exactly one worker to own a job at any moment, a way to recover a job
whose worker died, and protection against a slow worker overwriting the
result of the worker that replaced it.

## Decision

`JobStore.claim` (src/file_pipeline/jobs.py) uses DynamoDB conditional
writes:

- no record yet: create it with status `processing`, a random lease
  token, and an expiry time;
- final status: never reprocess (a redelivery only retries a failed
  notification);
- `processing` with a live lease: leave the message for a later retry;
- `processing` with an expired lease, or `failed`: take it over, with
  the condition re-checked in the same write so only one of several
  racing workers wins.

Every later write (`finalize_*`, `mark_failed`,
`mark_notification_result`) is conditioned on
`lease_owner == <this worker's token>`. The diagram
[job-lifecycle.svg](../job-lifecycle.svg) shows the resulting states.

## Consequences

- A worker whose lease was taken over cannot change the job record;
  its write fails the condition and it backs off.
- S3 report and curated writes are not conditioned on the lease. A
  stale worker can still overwrite them, but for the same job it writes
  the same bytes, because processing is deterministic.
- Recovery after a crash waits for the lease to expire (see
  [0003](0003-timeout-lease-visibility-ordering.md)).

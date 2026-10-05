# 0008. The rejection-rate limit lives in the schema

Status: accepted (2026-10-04)

## Context

A file with 90% bad rows used to finish as `completed_with_rejections`
and produce totals from the few rows that passed. That file almost
certainly has a systemic problem (wrong export, shifted columns), and
its totals should not be trusted.

## Decision

The schema's optional `max_rejection_rate` (the sales schema uses 0.5)
fails a file whose share of rejected rows is strictly above the limit:
status `validation_failed`, `error_code` `rejection_rate_exceeded`,
with a message such as "3 of 5 rows rejected (60%), above the 50%
limit". Reports are still written so the reasons stay visible. A file
with zero valid rows now also records an error code, `no_valid_rows`.

The limit is in the schema rather than an environment variable or CLI
flag, so the local CLI and the Lambda can never disagree about it.

## Consequences

- Exactly 50% still completes; the comparison is strictly greater.
- `samples/mixed_valid_invalid.csv` had 8 of 10 rows rejected, so 7
  valid rows were appended to keep it the partly-rejected example;
  `samples/high_rejection_rate.csv` shows the limit.
- Files that fail the limit write no curated rows, so Athena never sees
  them.

# 0001. Job IDs are derived from the S3 object version

Status: accepted (original build; recorded 2026-10-04)

## Context

S3 event notifications and SQS both deliver at least once. The same
upload can trigger the Lambda several times, sometimes concurrently.
A randomly generated job ID would make every delivery look like a new
job, and nothing would stop the same file from being processed twice.

## Decision

`job_id = sha256(bucket + "\n" + key + "\n" + version_id)`
(`jobs.compute_job_id`). The input bucket is versioned, so a version ID
names one exact set of bytes, and the Lambda always reads that exact
version.

## Consequences

- Every delivery of the same event maps to the same job record, which
  is what makes the claim logic in [0004](0004-expiring-lease-claims.md)
  able to detect duplicates and concurrent deliveries.
- Report keys (`reports/{job_id}/...`) are deterministic too, so a
  retry overwrites its own earlier output rather than adding a copy.
- Uploading the same bytes again, under any key, creates a new version
  and therefore a new job. Recognizing that case needs a second
  identity based on content: see
  [0009](0009-duplicate-content-fingerprints.md).

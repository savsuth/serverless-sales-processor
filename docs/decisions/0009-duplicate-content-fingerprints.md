# 0009. Duplicate uploads are recognized by a fingerprint of their content

Status: accepted (2026-10-04)

## Context

Job identity is per S3 object version
([0001](0001-deterministic-job-ids.md)). Uploading the same file again,
under a new name or the same one, is a new job, and its rows were
counted again by anyone adding up reports, and now by Athena.

## Decision

While streaming the file, the processor computes the SHA-256 of its
exact bytes at no extra read. Before a job that produced totals writes
any output, `JobStore.claim_content` records the fingerprint with a
conditional put of an item keyed `content#<sha256>` in the existing
jobs table:

- first job with this content: it proceeds;
- another job already completed this content: the new job ends as
  `duplicate_content`, with `duplicate_of` naming the original, and
  writes no reports or curated rows;
- the original job has not finished: the new job is recorded as
  `failed` with `waiting_for_original_job` (logged at INFO, so no error
  alarm) and retried later.

Files that fail validation never record a fingerprint: they produce no
totals, so re-running one cannot double count anything.

## Consequences

- No new table, index, or IAM permission: the Lambda already had
  `GetItem`, `PutItem`, and `UpdateItem` on the table. Real job IDs are
  bare hex, so `content#...` keys can never collide with them.
- Content is compared byte for byte: the same data with different line
  endings or column order is not a duplicate.
- If the original job never finishes, the waiting duplicate also
  exhausts its retries and lands in the dead-letter queue; redriving
  both resolves it.
- Uploads processed before this change have no fingerprint, so
  re-uploading one is processed normally, which is how existing files
  can be loaded into Athena once.

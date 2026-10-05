# 0010. Curated data is gzip JSON Lines, partitioned by month with partition projection

Status: accepted (2026-10-04)

## Context

Reports summarize one file at a time. Questions across uploads, such
as revenue by product this year, need the individual rows in a form a
query engine can read, without double counting and without a running
database.

## Decision

For each completed job with new content, the Lambda writes every valid
row to
`curated/<schema>/month=YYYY-MM/<job_id>.json.gz` in the output bucket.
Each row has its parsed columns, computed measures such as `revenue`,
and lineage (`job_id`, `source_row_number`). Terraform defines a Glue
table over that prefix and an Athena workgroup to query it.

- **JSON Lines, not CSV**: product names can contain commas, quotes,
  and line breaks, which Athena's CSV readers do not handle reliably.
- **Not Parquet**: it needs pyarrow, which the dependency-free Lambda
  package cannot include.
- **Numbers as plain JSON decimals**, read into `decimal(38,18)`
  columns, so Athena's sums match the Decimal totals in the reports.
- **Partition projection** on `month`: Athena computes partitions from
  table properties, so a new month is queryable immediately with no
  crawler to run or pay for.
- **Cost guards**: the workgroup cancels any query scanning more than
  1 GiB, and query results expire from their bucket after 7 days.

## Consequences

- Files are deterministic, so retries overwrite them identically.
- Rows dated outside `athena_month_range` (default `2000-01,NOW`) are
  stored but not visible to queries until the range is widened.
- Decimal values with more than 18 digits after the point, or integers
  beyond Athena's `bigint`, do not fit the column types exactly.
- Athena column types are defined in Terraform and in `curated.py`; a
  test keeps the two in step.
- Validated against the real Athena service only once deployed.

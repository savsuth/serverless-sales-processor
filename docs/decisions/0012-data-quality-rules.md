# 0012. Case-insensitive names and warnings that never change totals

Status: accepted (2026-10-05)

## Context

"Widget" and "widget" were counted as two products. Repeated rows and
price typos (999.00 for 9.99) were accepted silently.

## Decision

- A string column can be `case_insensitive` (sales: `product`). Names
  are compared with `lower()`, matching Athena's `lower()`. Reports show
  the alphabetically first spelling. "First spelling seen" was rejected
  after the property tests showed it lets row order change a report.
  Curated rows keep each row's own spelling; the saved Athena queries
  group by `lower(product)` and show `min(product)`, the same rule, and a
  property test checks they reproduce the report.
- `warnings` in the schema enables `duplicate_rows` (a valid row whose
  values all equal an earlier one's; numbers compared by value) and
  `outliers` (a value more than `factor` times above or below its
  group's median; groups under 3 rows skipped). Sales uses factor 10 on
  `unit_price` per `product`.
- Warnings never change totals or status. They appear in `summary.json`
  (exact count, up to 20 row numbers), on the job record, in the
  notification, and in the CLI output.

## Consequences

- Reports may show an all-caps spelling ("WIDGET") if any row uses it.
- Summaries gained a `warnings` block; snapshots changed only by that
  addition.
- Warning row numbers depend on row order by nature; their counts do
  not.

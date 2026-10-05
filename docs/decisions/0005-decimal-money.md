# 0005. Money is computed with Decimal and written as plain decimal strings

Status: accepted (original build; recorded 2026-10-04)

## Context

Binary floating point cannot represent most decimal prices exactly:
`0.1 + 0.2` is not `0.3`. Totals over many rows drift, and the local
CLI and the Lambda must produce byte-identical reports.

## Decision

Decimal columns are parsed with Python's `Decimal`, only from plain
notation (no exponent, no `NaN`/`Infinity`). Arithmetic stays in
`Decimal` at its default 28-significant-digit context, and amounts are
written as plain strings via `format(value, "f")`, never as JSON
floats.

## Consequences

- Exact arithmetic within 28 significant digits. Totals beyond that
  precision are rounded by the Decimal context; sales data is far from
  that limit.
- `summary.json` holds money as strings (`"29.97"`), which consumers
  must parse as decimals, not floats.
- Curated rows for Athena write the same values as JSON numbers in
  plain notation so Athena can read them into `decimal` columns; see
  [0010](0010-curated-json-lines-for-athena.md).

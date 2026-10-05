# 0007. Validation rules live in a JSON schema file

Status: accepted (2026-10-04)

## Context

The required columns, per-value rules, and totals were hard-coded for
sales data in `processor.py`. Supporting another kind of CSV meant
editing the engine, and the Athena table needs the same column
definitions the Lambda uses.

## Decision

A schema file (`src/file_pipeline/schemas/<name>.json`) lists each
column's type (`date`, `string`, `integer`, `decimal`) and options, the
column to group by, the measures to add up, an optional rejection-rate
limit, and optional curated output. Rejection codes are generated from
column names (`invalid_<name>`, `empty_<name>`, ...), so the sales
schema reproduces the original codes exactly. Each deployment uses one
schema (`schema_name` in Terraform, `SCHEMA_NAME` in the Lambda,
`--schema` on the CLI).

The format is JSON, not YAML: the Lambda package is the plain contents
of `src/` with no third-party libraries, and YAML would need PyYAML.

## Consequences

- The refactor was checked against snapshot tests and property-based
  tests written beforehand; output stayed byte-identical.
- Terraform reads the same file to define the Athena table's columns;
  a test runs `terraform console` to check it matches the Python side.
- JSON allows no comments, so the schema format is documented in
  `schema.py`'s docstring.
- Choosing a schema per S3 folder (several datasets in one deployment)
  is not supported yet.

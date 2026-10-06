# 0011. Input files are recognized in code, by content where possible

Status: accepted (2026-10-05)

## Context

S3's event filter matched the suffix `.csv` case-sensitively, so
`Sales.CSV` was silently never processed. Exports also arrive gzipped,
or separated by semicolons (common in European locales) or tabs.

## Decision

- The bucket notification sends every new object to the queue; the
  Lambda processes keys ending in `.csv` or `.csv.gz` in any letter case
  (`storage.is_input_key`) and logs and ignores anything else, without a
  job record.
- Gzip is detected from the first two bytes, not the name, and
  decompressed while streaming. The size limit and the content
  fingerprint ([0009](0009-duplicate-content-fingerprints.md)) apply to
  the decompressed CSV.
- The schema lists accepted separators (`delimiters`; sales: comma,
  semicolon, tab). For each file the first separator that splits the
  header into all required columns is used.

## Consequences

- Every object in the input bucket costs one Lambda invocation, even
  when it is ignored.
- A compression bomb stops at the size limit; a gzipped copy of an
  already-processed file is a duplicate.
- Comma files parse exactly as before; a property test checks that the
  same rows written with any accepted separator give an identical report.
- A semicolon file with decimal commas (`9,99`) still rejects those
  prices; that format is not accepted.

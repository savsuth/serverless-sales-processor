# 0020. The processor's memory is sized for CPU, from measurements

Status: accepted (2026-10-06)

## Context

The processor ran at 512 MB, chosen as a guess. Measured on the
deployed stack ([docs/costs.md](../costs.md)), processing is CPU-bound
at about 0.12 ms per row. Time follows rows, not bytes. A 10 MiB file
of the shortest valid rows (about 617,000 rows) would need about 67 s,
past the 60 s timeout. Such a file is within the documented limit, yet
it would fail every attempt and end `dead_lettered`. Peak memory for a
10 MB file was only 212 MB.

## Decision

The default memory is 1024 MB. Lambda gives CPU in proportion to
memory, which roughly halves the time: about 34 s for the worst case.
The timeout stays at 60 s, so its ordering below the lease
([0003](0003-timeout-lease-visibility-ordering.md)) is unchanged.

## Consequences

- Cost per file barely changes: CPU-bound work at twice the memory
  takes about half the time.
- If the size limit or the schema grows, re-measure before raising the
  timeout. Memory up to 1,769 MB (one full vCPU) is the cheaper lever.

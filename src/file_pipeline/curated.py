"""Curated output: every valid row of a completed job, written as
queryable data for Athena.

Rows are gzip-compressed JSON Lines, one file per calendar month of the
schema's `curated.partition_by_month` date column:

    curated/<schema>/month=YYYY-MM/<job_id>.json.gz

Each line holds the schema's columns (parsed and trimmed), each measure
that is not itself a column (e.g. `revenue`, computed with Decimal just
like the summary), and lineage: the `job_id` and the row's
`source_row_number` in the uploaded file, so any number Athena returns
can be traced back to the upload and line it came from.

Why JSON Lines rather than CSV: values such as product names can contain
commas, quotes, and line breaks, which Athena's CSV readers do not
handle reliably; JSON escapes them. Numbers are written as JSON numbers
in plain decimal notation, so Athena reads `decimal` columns exactly.
Why not Parquet: it needs pyarrow, which the dependency-free Lambda
package cannot include (see infra/lambda.tf).

Files are byte-for-byte deterministic for a given job (rows in file
order, gzip timestamp fixed at 0), so a retry overwrites them with
identical objects. Athena column types for these fields are defined in
infra/athena.tf and must match `athena_columns()` below.
"""

from __future__ import annotations

import gzip
import io
import json
import tempfile
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

from file_pipeline.schema import Schema

CURATED_PREFIX = "curated"

_SPOOL_BYTES = 1024 * 1024

ATHENA_TYPES = {
    "date": "date",
    "string": "string",
    "integer": "bigint",
    "decimal": "decimal(38,18)",
}


def curated_key(schema: Schema, month: str, job_id: str) -> str:
    return f"{CURATED_PREFIX}/{schema.name}/month={month}/{job_id}.json.gz"


def athena_columns(schema: Schema) -> list[tuple[str, str]]:
    """(name, Athena type) for every field of a curated row, in order.
    The `month` partition is not a field: it comes from the S3 path."""
    columns = [(c.name, ATHENA_TYPES[c.type]) for c in schema.columns]
    column_names = set(schema.column_names)
    columns += [
        (m.name, ATHENA_TYPES["decimal" if m.is_decimal else "integer"])
        for m in schema.measures
        if m.name not in column_names
    ]
    return [*columns, ("job_id", "string"), ("source_row_number", "bigint")]


def _json_value(value: Any) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, int):
        return str(value)
    return json.dumps(value)


class CuratedWriter:
    """Collects a job's valid rows, split by month, in temporary files.
    Nothing is uploaded here: the caller decides, once the job's outcome
    is known, whether to write `files()` to S3 or discard them."""

    def __init__(self, schema: Schema, job_id: str) -> None:
        assert schema.curated_partition_column is not None
        self._partition_column: str = schema.curated_partition_column
        self._schema = schema
        self._job_id = job_id
        column_names = set(schema.column_names)
        self._measure_names = [m.name for m in schema.measures if m.name not in column_names]
        self._months: dict[str, tuple[Any, gzip.GzipFile]] = {}

    def add(self, row_number: int, values: dict[str, Any], measures: dict[str, Any]) -> None:
        fields = [(name, values[name]) for name in self._schema.column_names]
        fields += [(name, measures[name]) for name in self._measure_names]
        fields += [("job_id", self._job_id), ("source_row_number", row_number)]
        line = "{" + ",".join(f"{json.dumps(k)}:{_json_value(v)}" for k, v in fields) + "}\n"

        month = values[self._partition_column][:7]  # YYYY-MM
        self._month_file(month).write(line.encode("utf-8"))

    def _month_file(self, month: str) -> gzip.GzipFile:
        if month not in self._months:
            # Owned by this writer for its lifetime; closed in close().
            spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_BYTES)  # noqa: SIM115
            self._months[month] = (spool, gzip.GzipFile(fileobj=spool, mode="wb", mtime=0))
        return self._months[month][1]

    def files(self) -> Iterator[tuple[str, bytes]]:
        """(S3 key, gzip bytes) per month, in month order."""
        for month in sorted(self._months):
            spool, gz = self._months[month]
            gz.close()  # flushes the gzip trailer; the spool stays open
            spool.seek(0)
            yield curated_key(self._schema, month, self._job_id), spool.read()

    def close(self) -> None:
        for spool, gz in self._months.values():
            gz.close()
            spool.close()
        self._months.clear()

    def __enter__(self) -> CuratedWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def read_curated_file(body: bytes) -> list[dict[str, Any]]:
    """Decodes one curated file, numbers as Decimal (for tests/tooling)."""
    text = gzip.decompress(body).decode("utf-8")
    return [json.loads(line, parse_float=Decimal) for line in io.StringIO(text)]

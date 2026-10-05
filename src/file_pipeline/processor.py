"""Pure CSV validation and aggregation logic.

No AWS or filesystem-path dependencies live here: everything operates on
file-like byte streams and text writers so the exact same code runs in the
local CLI runner (local.py) and the Lambda handler (handler.py).

Which columns are required, how each value is validated, and what gets
added up all come from a schema (see schema.py); the default is the
bundled sales schema.

Design decisions (documented here because the task spec leaves them open):

* Column matching is case-insensitive and ignores surrounding whitespace,
  e.g. " Date" and "date" both satisfy the "date" requirement.
* The field separator is chosen per file from the schema's `delimiters`:
  the first one that splits the header into all the required columns.
* Duplicate header names (after normalization) make the whole file
  malformed, because we can no longer say which column a value belongs to.
* Extra, unrecognized columns are allowed. Their values are preserved
  verbatim in rejected_rows.csv (since that file echoes the raw row) but
  are otherwise ignored -- they never affect validation or aggregation.
* A row with a different number of fields than the header ("ragged" CSV)
  is treated as an invalid *row* (skipped, recorded with a reason), not a
  file-level malformed-CSV error, because the header itself still parsed.
* A structural parse failure (unterminated quote, undecodable bytes, an
  empty file, missing required columns, duplicate headers) is a
  file-level error: the whole file is rejected and no outputs are
  produced. See MalformedCSVError / InputTooLargeError.
* If the file parses fine but zero rows pass validation, the caller
  should mark the job validation_failed even though summary.json and
  rejected_rows.csv are still produced (there's nothing wrong with the
  file's structure, just its content).
* Monetary values are Decimal throughout and serialized as plain decimal
  strings (no scientific notation, no float) to avoid float rounding
  error and precision loss.
* A gzip-compressed file is recognized by its first two bytes (not its
  name) and decompressed while streaming. The size limit and the content
  fingerprint both apply to the decompressed CSV, so a compression bomb
  is stopped at the limit and the same data compressed or not is the
  same content.
* rejected_rows.csv values are defended against spreadsheet formula
  injection: any field beginning with '=', '+', '-', '@', a tab, or a
  carriage return is prefixed with a leading apostrophe before being
  written, per OWASP's CSV injection guidance. This changes how the cell
  looks if re-imported programmatically, but keeps it inert when opened
  in Excel/Sheets/Numbers.
"""

from __future__ import annotations

import csv
import datetime
import gzip
import hashlib
import io
import itertools
import re
import statistics
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import IO, Any, TextIO

from file_pipeline.schema import Column, OutlierRule, Schema, load_schema

MAX_INPUT_BYTES = 10 * 1024 * 1024  # 10 MiB

_CHUNK_SIZE = 64 * 1024

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_UNSIGNED_INTEGER_RE = re.compile(r"^\d+$")
_SIGNED_INTEGER_RE = re.compile(r"^-?\d+$")
_DECIMAL_RE = re.compile(r"^-?\d+(\.\d+)?$")
_NON_FINITE_TOKENS = {
    "nan",
    "inf",
    "+inf",
    "-inf",
    "infinity",
    "+infinity",
    "-infinity",
}

_FORMULA_INJECTION_PREFIXES = ("=", "+", "-", "@", "\t", "\r")

REJECTED_CSV_EXTRA_COLUMNS = ("row_number", "rejection_reason")

# A warning lists at most this many row numbers; its count is always exact.
MAX_WARNING_ROW_NUMBERS = 20

STATUS_COMPLETED = "completed"
STATUS_COMPLETED_WITH_REJECTIONS = "completed_with_rejections"
STATUS_VALIDATION_FAILED = "validation_failed"


class MalformedCSVError(Exception):
    """The file as a whole cannot be safely processed; reject it entirely."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class InputTooLargeError(Exception):
    """The input stream exceeded the configured byte limit."""

    def __init__(self, max_bytes: int) -> None:
        super().__init__(f"Input exceeded the {max_bytes}-byte limit")
        self.max_bytes = max_bytes


class GroupTotals(dict):
    """One group's measures, e.g. {"quantity": 5, "revenue": Decimal("49.95")}.
    Measures are also readable as attributes (totals.revenue)."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


@dataclass
class ProcessingResult:
    """Counts, status, and totals for one file.

    Results are also readable under the names the summary uses, which
    come from the schema: `by_<group_by>` (e.g. `by_product`) for the
    per-group totals and `total_<measure>` (e.g. `total_revenue`) for
    each grand total.

    `error_code` / `error_message` say why a structurally valid file is
    `validation_failed` (`no_valid_rows` or `rejection_rate_exceeded`);
    both are None otherwise.

    `content_sha256` is the SHA-256 of the file's exact bytes, used to
    recognize the same content uploaded again (see jobs.claim_content).

    `warnings` holds the schema's warning checks that ran, e.g.
    {"duplicate_rows": {"count": 1, "row_numbers": [7]}}."""

    status: str
    valid_row_count: int
    rejected_row_count: int
    schema: Schema
    totals: dict[str, int | Decimal] = field(default_factory=dict)
    groups: dict[str, GroupTotals] = field(default_factory=dict)
    error_code: str | None = None
    error_message: str | None = None
    content_sha256: str = ""
    warnings: dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name: str) -> Any:
        schema = self.__dict__.get("schema")
        if schema is not None:
            if name == f"by_{schema.group_by}":
                return self.groups
            if name.startswith("total_") and name[len("total_") :] in self.totals:
                return self.totals[name[len("total_") :]]
        raise AttributeError(name)


GZIP_MAGIC = b"\x1f\x8b"


class _StreamReader(io.RawIOBase):
    """Adapts any object with .read(n) (a file, an S3 StreamingBody) to
    the raw-stream interface io.BufferedReader needs."""

    def __init__(self, source: IO[bytes]) -> None:
        self._source = source

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray) -> int:  # type: ignore[override]
        data = self._source.read(len(b))
        b[: len(data)] = data
        return len(data)


def _decompressed(binary_stream: IO[bytes]) -> IO[bytes]:
    buffered = io.BufferedReader(_StreamReader(binary_stream))
    if buffered.peek(len(GZIP_MAGIC))[: len(GZIP_MAGIC)] == GZIP_MAGIC:
        return gzip.GzipFile(fileobj=buffered, mode="rb")
    return buffered


class _CountingRawReader(io.RawIOBase):
    """Wraps any object with .read(n) and enforces a byte ceiling while
    streaming, so an oversized or mislabeled input is caught mid-read
    instead of only after being fully buffered. Also fingerprints the
    bytes as they pass, so no second read is needed."""

    def __init__(self, source: IO[bytes], max_bytes: int) -> None:
        self._source = source
        self._max_bytes = max_bytes
        self._read_bytes = 0
        self.sha256 = hashlib.sha256()

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray) -> int:  # type: ignore[override]
        data = self._source.read(len(b))
        if not data:
            return 0
        n = len(data)
        b[:n] = data
        self.sha256.update(data)
        self._read_bytes += n
        if self._read_bytes > self._max_bytes:
            raise InputTooLargeError(self._max_bytes)
        return n


def _sanitize_csv_field(value: str) -> str:
    if value and value[0] in _FORMULA_INJECTION_PREFIXES:
        return "'" + value
    return value


def _decimal_to_str(value: Decimal) -> str:
    return format(value, "f")


def _open_text_stream(raw: _CountingRawReader) -> TextIO:
    buffered = io.BufferedReader(raw)
    return io.TextIOWrapper(buffered, encoding="utf-8-sig", newline="")


def _normalize_header_name(name: str) -> str:
    return name.strip().lower()


def _detect_delimiter(header_line: str, schema: Schema) -> str:
    """The first of the schema's delimiters that splits the header line
    into all the required columns. Falls back to the first delimiter, so
    a file matching none fails header validation with the usual error."""
    required = set(schema.column_names)
    for delimiter in schema.delimiters:
        fields = next(csv.reader([header_line], delimiter=delimiter), [])
        if required <= {_normalize_header_name(f) for f in fields}:
            return delimiter
    return schema.delimiters[0]


def _validate_header(header: list[str], schema: Schema) -> dict[str, int]:
    normalized = [_normalize_header_name(h) for h in header]

    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in normalized:
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    if duplicates:
        raise MalformedCSVError(
            "duplicate_header",
            f"Duplicate column name(s) in header: {sorted(duplicates)}",
        )

    column_index = {name: idx for idx, name in enumerate(normalized)}
    missing = [c for c in schema.column_names if c not in column_index]
    if missing:
        raise MalformedCSVError(
            "missing_required_columns",
            f"Missing required column(s): {missing}",
        )
    return column_index


def _parse_value(column: Column, raw: str) -> tuple[Any, str | None]:
    """Returns (value, rejection_reason) for one already-trimmed field.
    The rejection codes are documented in schema.py."""
    if column.type == "date":
        if not _DATE_RE.match(raw):
            return None, f"invalid_{column.name}"
        try:
            year, month, day = (int(p) for p in raw.split("-"))
            datetime.date(year, month, day)
        except ValueError:
            return None, f"invalid_{column.name}"
        return raw, None

    if column.type == "string":
        if column.required and not raw:
            return None, f"empty_{column.name}"
        return raw, None

    if column.type == "integer":
        pattern = _SIGNED_INTEGER_RE if column.signed else _UNSIGNED_INTEGER_RE
        if not pattern.match(raw):
            return None, f"invalid_{column.name}"
        value = int(raw)
        if column.positive and value <= 0:
            return None, f"non_positive_{column.name}"
        return value, None

    # decimal
    if raw.lower() in _NON_FINITE_TOKENS:
        return None, f"non_finite_{column.name}"
    if not _DECIMAL_RE.match(raw):
        return None, f"invalid_{column.name}"
    if column.non_negative and raw.startswith("-"):
        return None, f"negative_{column.name}"
    return Decimal(raw), None


def _validate_row(
    row: list[str], column_index: dict[str, int], header_len: int, schema: Schema
) -> tuple[dict[str, Any] | None, str | None]:
    """Returns (values, rejection_reason): the parsed value of every
    schema column on success, or the first failing column's reason."""
    if len(row) != header_len:
        return None, "row_length_mismatch"

    values: dict[str, Any] = {}
    for column in schema.columns:
        value, reason = _parse_value(column, row[column_index[column.name]].strip())
        if reason is not None:
            return None, reason
        values[column.name] = value
    return values, None


def _comparable(value: Any, ignore_case: bool) -> Any:
    if isinstance(value, Decimal):
        return value.normalize()  # 9.99 and 9.990 are the same price
    if ignore_case:
        return value.lower()
    return value


class _DuplicateRows:
    """Spots valid rows whose values all equal an earlier valid row's.
    Keeps a 16-byte digest per distinct row, not the row itself."""

    def __init__(self, schema: Schema) -> None:
        self._columns = [(c.name, c.case_insensitive) for c in schema.columns]
        self._seen: set[bytes] = set()
        self.row_numbers: list[int] = []

    def add(self, row_number: int, values: dict[str, Any]) -> None:
        key = tuple(_comparable(values[name], ignore_case) for name, ignore_case in self._columns)
        digest = hashlib.blake2b(repr(key).encode(), digest_size=16).digest()
        if digest in self._seen:
            self.row_numbers.append(row_number)
        else:
            self._seen.add(digest)


class _Outliers:
    """Collects one numeric column per group, then flags values more than
    `factor` times away from their group's median."""

    def __init__(self, rule: OutlierRule, schema: Schema) -> None:
        self._rule = rule
        self._ignore_case = schema.column(rule.per).case_insensitive
        self._groups: dict[Any, list[tuple[Decimal, int]]] = {}

    def add(self, row_number: int, values: dict[str, Any]) -> None:
        key = _comparable(values[self._rule.per], self._ignore_case)
        value = Decimal(values[self._rule.column])
        self._groups.setdefault(key, []).append((value, row_number))

    def row_numbers(self) -> list[int]:
        factor = self._rule.factor
        flagged = []
        for entries in self._groups.values():
            if len(entries) < 3:
                continue
            median = statistics.median(value for value, _ in entries)
            if median <= 0:
                continue
            flagged += [
                n for value, n in entries if value > median * factor or value * factor < median
            ]
        return sorted(flagged)


def _warning(row_numbers: list[int], **extra: Any) -> dict[str, Any]:
    return {
        **extra,
        "count": len(row_numbers),
        "row_numbers": row_numbers[:MAX_WARNING_ROW_NUMBERS],
    }


def process_csv(
    binary_stream: IO[bytes],
    rejected_csv_writer_target: TextIO,
    *,
    max_bytes: int = MAX_INPUT_BYTES,
    schema: Schema | None = None,
    on_valid_row: Callable[[int, dict[str, Any], dict[str, Any]], None] | None = None,
) -> ProcessingResult:
    """Streams and validates a CSV file, writing rejected rows as it goes.

    `on_valid_row(row_number, values, measures)`, if given, is called for
    every valid row with its parsed column values and computed measures
    (used to build curated output; see curated.py).

    `rejected_csv_writer_target` should be a caller-owned, writable text
    stream (e.g. a SpooledTemporaryFile) that the caller only persists to
    its final destination (disk, S3) after this function returns
    successfully. On MalformedCSVError / InputTooLargeError, whatever was
    written to it is incomplete and must be discarded by the caller.
    """
    schema = schema or load_schema()
    raw = _CountingRawReader(_decompressed(binary_stream), max_bytes)
    try:
        text_stream = _open_text_stream(raw)
        header_line = text_stream.readline()
        if not header_line:
            raise MalformedCSVError("empty_file", "CSV file is empty")
        delimiter = _detect_delimiter(header_line, schema)
        reader = csv.reader(itertools.chain([header_line], text_stream), delimiter=delimiter)
        header = next(reader)

        column_index = _validate_header(header, schema)
        header_len = len(header)

        rejected_writer = csv.writer(rejected_csv_writer_target)
        rejected_writer.writerow(
            [_sanitize_csv_field(h) for h in header] + list(REJECTED_CSV_EXTRA_COLUMNS)
        )

        valid_row_count = 0
        rejected_row_count = 0
        totals: dict[str, int | Decimal] = {
            m.name: Decimal("0") if m.is_decimal else 0 for m in schema.measures if m.total
        }
        # Groups are keyed by the value as compared (lowercased for a
        # case_insensitive column, matching Athena's lower()) and reported
        # under the alphabetically first spelling, so row order never
        # changes the report.
        group_case_insensitive = schema.column(schema.group_by).case_insensitive
        groups_by_key: dict[str, GroupTotals] = {}
        group_names: dict[str, str] = {}
        duplicate_rows = _DuplicateRows(schema) if schema.warn_duplicate_rows else None
        outliers = _Outliers(schema.outliers, schema) if schema.outliers else None

        row_number = 0
        for row in reader:
            row_number += 1
            if not row or row == [""]:
                # csv module yields [] for a fully blank physical line;
                # treat it as an empty row rather than a length mismatch.
                continue

            values, reason = _validate_row(row, column_index, header_len, schema)

            if reason is not None:
                rejected_row_count += 1
                sanitized_row = [_sanitize_csv_field(v) for v in row]
                rejected_writer.writerow([*sanitized_row, row_number, reason])
                continue

            assert values is not None
            valid_row_count += 1
            row_measures = {}
            for measure in schema.measures:
                amount = 1
                for column_name in measure.multiply:
                    amount = amount * values[column_name]
                row_measures[measure.name] = amount
                if measure.total:
                    totals[measure.name] += amount
            group_value = values[schema.group_by]
            group_key = group_value.lower() if group_case_insensitive else group_value
            group_names[group_key] = min(group_names.get(group_key, group_value), group_value)

            if on_valid_row is not None:
                on_valid_row(row_number, values, row_measures)
            if duplicate_rows is not None:
                duplicate_rows.add(row_number, values)
            if outliers is not None:
                outliers.add(row_number, values)

            existing = groups_by_key.get(group_key)
            if existing is None:
                groups_by_key[group_key] = GroupTotals(row_measures)
            else:
                for name, amount in row_measures.items():
                    existing[name] += amount
    except UnicodeDecodeError as exc:
        raise MalformedCSVError("invalid_encoding", str(exc)) from exc
    except csv.Error as exc:
        raise MalformedCSVError("csv_parse_error", str(exc)) from exc
    except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
        raise MalformedCSVError(
            "invalid_gzip", "File starts like gzip but could not be decompressed"
        ) from exc

    warnings: dict[str, Any] = {}
    if duplicate_rows is not None:
        warnings["duplicate_rows"] = _warning(duplicate_rows.row_numbers)
    if outliers is not None:
        assert schema.outliers is not None
        warnings["outliers"] = _warning(outliers.row_numbers(), column=schema.outliers.column)

    error_code = error_message = None
    row_count = valid_row_count + rejected_row_count
    limit = schema.max_rejection_rate
    if valid_row_count == 0:
        status = STATUS_VALIDATION_FAILED
        error_code, error_message = "no_valid_rows", "No row passed validation"
    elif limit is not None and rejected_row_count > limit * row_count:
        status = STATUS_VALIDATION_FAILED
        error_code = "rejection_rate_exceeded"
        error_message = (
            f"{rejected_row_count} of {row_count} rows rejected "
            f"({_percent(Decimal(rejected_row_count) / row_count)}), "
            f"above the {_percent(limit)} limit"
        )
    elif rejected_row_count == 0:
        status = STATUS_COMPLETED
    else:
        status = STATUS_COMPLETED_WITH_REJECTIONS

    return ProcessingResult(
        status=status,
        valid_row_count=valid_row_count,
        rejected_row_count=rejected_row_count,
        schema=schema,
        totals=totals,
        groups={group_names[key]: totals_ for key, totals_ in groups_by_key.items()},
        error_code=error_code,
        error_message=error_message,
        content_sha256=raw.sha256.hexdigest(),
        warnings=warnings,
    )


def _percent(rate: Decimal) -> str:
    """0.5 -> "50%", 0.125 -> "12.5%", 2/3 -> "66.7%"."""
    return _decimal_to_str(round(rate * 100, 1).normalize()) + "%"


def _serialize_amount(value: int | Decimal) -> int | str:
    return _decimal_to_str(value) if isinstance(value, Decimal) else value


def build_summary_dict(result: ProcessingResult) -> dict:
    schema = result.schema
    summary: dict[str, Any] = {
        "valid_row_count": result.valid_row_count,
        "rejected_row_count": result.rejected_row_count,
        f"by_{schema.group_by}": {
            key: {name: _serialize_amount(amount) for name, amount in measures.items()}
            for key, measures in sorted(result.groups.items())
        },
    }
    for name, amount in result.totals.items():
        summary[f"total_{name}"] = _serialize_amount(amount)
    if result.warnings:
        summary["warnings"] = result.warnings
    return summary


def summary_json_bytes(result: ProcessingResult) -> bytes:
    """Canonical summary.json bytes -- used by both local.py and
    storage.py so local and AWS output are byte-for-byte identical."""
    import json

    return (json.dumps(build_summary_dict(result), indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )

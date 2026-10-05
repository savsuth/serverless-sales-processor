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
import hashlib
import io
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import IO, Any, TextIO

from file_pipeline.schema import Column, Schema, load_schema

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
    recognize the same content uploaded again (see jobs.claim_content)."""

    status: str
    valid_row_count: int
    rejected_row_count: int
    schema: Schema
    totals: dict[str, int | Decimal] = field(default_factory=dict)
    groups: dict[str, GroupTotals] = field(default_factory=dict)
    error_code: str | None = None
    error_message: str | None = None
    content_sha256: str = ""

    def __getattr__(self, name: str) -> Any:
        schema = self.__dict__.get("schema")
        if schema is not None:
            if name == f"by_{schema.group_by}":
                return self.groups
            if name.startswith("total_") and name[len("total_") :] in self.totals:
                return self.totals[name[len("total_") :]]
        raise AttributeError(name)


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


def process_csv(
    binary_stream: IO[bytes],
    rejected_csv_writer_target: TextIO,
    *,
    max_bytes: int = MAX_INPUT_BYTES,
    schema: Schema | None = None,
) -> ProcessingResult:
    """Streams and validates a CSV file, writing rejected rows as it goes.

    `rejected_csv_writer_target` should be a caller-owned, writable text
    stream (e.g. a SpooledTemporaryFile) that the caller only persists to
    its final destination (disk, S3) after this function returns
    successfully. On MalformedCSVError / InputTooLargeError, whatever was
    written to it is incomplete and must be discarded by the caller.
    """
    schema = schema or load_schema()
    raw = _CountingRawReader(binary_stream, max_bytes)
    try:
        text_stream = _open_text_stream(raw)
        reader = csv.reader(text_stream)
        try:
            header = next(reader)
        except StopIteration:
            raise MalformedCSVError("empty_file", "CSV file is empty") from None

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
        groups: dict[str, GroupTotals] = {}

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

            group_key = values[schema.group_by]
            existing = groups.get(group_key)
            if existing is None:
                groups[group_key] = GroupTotals(row_measures)
            else:
                for name, amount in row_measures.items():
                    existing[name] += amount
    except UnicodeDecodeError as exc:
        raise MalformedCSVError("invalid_encoding", str(exc)) from exc
    except csv.Error as exc:
        raise MalformedCSVError("csv_parse_error", str(exc)) from exc

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
        groups=groups,
        error_code=error_code,
        error_message=error_message,
        content_sha256=raw.sha256.hexdigest(),
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
    return summary


def summary_json_bytes(result: ProcessingResult) -> bytes:
    """Canonical summary.json bytes -- used by both local.py and
    storage.py so local and AWS output are byte-for-byte identical."""
    import json

    return (json.dumps(build_summary_dict(result), indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )

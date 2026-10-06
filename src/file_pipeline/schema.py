"""Dataset schemas: which columns a CSV must have, how each value is
validated, and what gets added up.

A schema is a JSON file. `sales` (schemas/sales.json) ships with the
package and is the default; `load_schema` also accepts a path to any
other schema file. JSON rather than YAML because the Lambda package is
the plain contents of src/ with no third-party libraries -- see
infra/lambda.tf.

Shape of a schema file:

    {
      "name": "sales",
      "columns": [
        {"name": "date", "type": "date"},
        {"name": "product", "type": "string", "required": true},
        {"name": "quantity", "type": "integer", "positive": true},
        {"name": "unit_price", "type": "decimal", "non_negative": true}
      ],
      "aggregation": {
        "group_by": "product",
        "measures": [
          {"name": "quantity", "multiply": ["quantity"]},
          {"name": "revenue", "multiply": ["quantity", "unit_price"], "total": true}
        ]
      },
      "max_rejection_rate": 0.5
    }

Column types and their options:

* `date`: `YYYY-MM-DD`, a real calendar date. Rejection: `invalid_<name>`.
* `string`: any text, trimmed. `required` rejects an empty value
  (`empty_<name>`). `case_insensitive` treats values that differ only
  in letter case as the same value when grouping and when spotting
  duplicate rows. Reports show the alphabetically first spelling, so row
  order never changes a report; curated rows keep each row's own
  spelling, and `GROUP BY lower(col)` with `min(col)` in Athena gives the
  same grouping.
* `integer`: digits only, no sign unless `signed`. `positive` rejects
  zero and below (`non_positive_<name>`). Otherwise `invalid_<name>`.
* `decimal`: plain decimal notation, no exponent. `NaN`/`Infinity`
  spellings are `non_finite_<name>`; `non_negative` rejects any value
  written with a minus sign, including `-0` (`negative_<name>`).
  Otherwise `invalid_<name>`.

Columns are checked in the order listed and the first failure is the
row's rejection reason. Each measure adds up, over every valid row, the
product of the listed numeric columns, per value of the `group_by`
column; `total: true` also reports a grand total as `total_<name>`.

`max_rejection_rate` (optional, 0 to 1) fails a file whose share of
rejected rows is strictly above it, even though some rows were valid:
its reports are still written, but its status is `validation_failed`.
Omit it to accept any share of rejected rows.

`version` (optional, default 1) numbers the rules. Bump it when a rule
changes; each job records the schema name and version it was processed
with, and `scripts/reprocess.py --older-than-version N` re-runs jobs
processed under older rules.

`delimiters` (optional, default `[","]`) lists the field separators a
file may use, in order of preference; the first one that splits the
header into all the required columns is used, so semicolon- or
tab-separated exports work without any setting per file.

`warnings` (optional) flags suspicious valid rows without rejecting
them or changing any total; the report lists how many and which rows:

    "warnings": {
      "duplicate_rows": true,
      "outliers": {"column": "unit_price", "per": "product", "factor": 10}
    }

`duplicate_rows` flags a row whose values all equal an earlier valid
row's (numbers compared by value, case-insensitive columns ignoring
case). `outliers` flags a row whose `column` value is more than `factor`
times above or below the median of that column for the same `per` value
in the file (e.g. 999.00 where a product usually costs 9.99); groups with
fewer than 3 rows, or a median of zero, are not checked.

`curated` (optional) makes the Lambda also write every valid row of a
completed job as queryable data for Athena, one file per calendar month
of the named date column (see curated.py):

    "curated": {"partition_by_month": "date"}

Column and measure names then become Athena column names, so they must
be plain identifiers (lowercase letters, digits, underscores), and
`month`, `job_id`, and `source_row_number` are reserved.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal
from functools import cache
from importlib import resources
from pathlib import Path
from typing import Any

DEFAULT_SCHEMA = "sales"

COLUMN_OPTIONS = {
    "date": frozenset(),
    "string": frozenset({"required", "case_insensitive"}),
    "integer": frozenset({"positive", "signed"}),
    "decimal": frozenset({"non_negative"}),
}
GROUPABLE_TYPES = frozenset({"date", "string"})
NUMERIC_TYPES = frozenset({"integer", "decimal"})

# Added to every curated row (curated.py), plus the `month` partition.
CURATED_RESERVED_NAMES = frozenset({"month", "job_id", "source_row_number"})
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


class SchemaError(Exception):
    """The schema file itself is invalid."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    required: bool = False
    case_insensitive: bool = False
    positive: bool = False
    signed: bool = False
    non_negative: bool = False


@dataclass(frozen=True)
class Measure:
    name: str
    multiply: tuple[str, ...]
    total: bool
    is_decimal: bool


@dataclass(frozen=True)
class OutlierRule:
    column: str
    per: str
    factor: Decimal


@dataclass(frozen=True)
class Schema:
    name: str
    columns: tuple[Column, ...]
    group_by: str
    measures: tuple[Measure, ...]
    max_rejection_rate: Decimal | None = None
    version: int = 1
    curated_partition_column: str | None = None
    delimiters: tuple[str, ...] = (",",)
    warn_duplicate_rows: bool = False
    outliers: OutlierRule | None = None

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)


def load_schema(name_or_path: str = DEFAULT_SCHEMA) -> Schema:
    """Loads a bundled schema by name (e.g. "sales") or a schema file by
    path (anything ending in .json)."""
    if name_or_path.endswith(".json"):
        text = Path(name_or_path).read_text(encoding="utf-8")
        return parse_schema(json.loads(text))
    return _load_bundled(name_or_path)


@cache
def _load_bundled(name: str) -> Schema:
    schema_file = resources.files("file_pipeline").joinpath("schemas", f"{name}.json")
    if not schema_file.is_file():
        raise SchemaError(f"No bundled schema named {name!r}")
    return parse_schema(json.loads(schema_file.read_text(encoding="utf-8")))


def parse_schema(data: dict[str, Any]) -> Schema:
    _require_keys(
        data,
        "schema",
        required={"name", "columns", "aggregation"},
        optional={"max_rejection_rate", "curated", "delimiters", "warnings", "version"},
    )
    name = data["name"]
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        # Also the curated S3 prefix and Athena table name.
        raise SchemaError(f"schema name must be lowercase letters, digits, underscores: {name!r}")

    columns = tuple(_parse_column(c) for c in _non_empty_list(data["columns"], "columns"))
    by_name = {c.name: c for c in columns}
    if len(by_name) != len(columns):
        raise SchemaError("column names must be unique")

    aggregation = data["aggregation"]
    _require_keys(aggregation, "aggregation", required={"group_by", "measures"})

    group_by = aggregation["group_by"]
    if group_by not in by_name:
        raise SchemaError(f"group_by refers to unknown column {group_by!r}")
    if by_name[group_by].type not in GROUPABLE_TYPES:
        raise SchemaError(f"group_by column {group_by!r} must be a date or string column")

    measures = tuple(
        _parse_measure(m, by_name) for m in _non_empty_list(aggregation["measures"], "measures")
    )
    if len({m.name for m in measures}) != len(measures):
        raise SchemaError("measure names must be unique")
    for measure in measures:
        # A measure may share a column's name only when it is exactly
        # that column (e.g. "quantity" summing the quantity column), so
        # a name always means one thing in reports and curated data.
        if measure.name in by_name and measure.multiply != (measure.name,):
            raise SchemaError(f"measure {measure.name!r} clashes with the column of that name")

    return Schema(
        name=name,
        columns=columns,
        group_by=group_by,
        measures=measures,
        max_rejection_rate=_parse_rate(data.get("max_rejection_rate")),
        version=_parse_version(data.get("version", 1)),
        curated_partition_column=_parse_curated(data.get("curated"), by_name, measures),
        delimiters=_parse_delimiters(data.get("delimiters", [","])),
        **_parse_warnings(data.get("warnings"), by_name),
    )


def _parse_warnings(value: Any, columns: dict[str, Column]) -> dict[str, Any]:
    if value is None:
        return {}
    _require_keys(value, "warnings", required=set(), optional={"duplicate_rows", "outliers"})
    duplicate_rows = value.get("duplicate_rows", False)
    if not isinstance(duplicate_rows, bool):
        raise SchemaError("warnings.duplicate_rows must be true or false")

    outliers = None
    if "outliers" in value:
        rule = value["outliers"]
        _require_keys(rule, "warnings.outliers", required={"column", "per", "factor"})
        column = columns.get(rule["column"])
        if column is None or column.type not in NUMERIC_TYPES:
            raise SchemaError(f"warnings.outliers column must be numeric: {rule['column']!r}")
        per = columns.get(rule["per"])
        if per is None or per.type not in GROUPABLE_TYPES:
            raise SchemaError(
                f"warnings.outliers per must be a date or string column: {rule['per']!r}"
            )
        factor = rule["factor"]
        if isinstance(factor, bool) or not isinstance(factor, int | float) or factor <= 1:
            raise SchemaError(f"warnings.outliers factor must be a number above 1: {factor!r}")
        outliers = OutlierRule(column=column.name, per=per.name, factor=Decimal(str(factor)))
    return {"warn_duplicate_rows": duplicate_rows, "outliers": outliers}


def _parse_version(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise SchemaError(f"version must be a whole number from 1 up: {value!r}")
    return value


def _parse_delimiters(value: Any) -> tuple[str, ...]:
    delimiters = tuple(_non_empty_list(value, "delimiters"))
    for d in delimiters:
        if not isinstance(d, str) or len(d) != 1 or d.isalnum() or d in "\"\r\n ":
            raise SchemaError(f"each delimiter must be one punctuation or tab character: {d!r}")
    if len(set(delimiters)) != len(delimiters):
        raise SchemaError("delimiters must be unique")
    return delimiters


def _parse_curated(
    value: Any, columns: dict[str, Column], measures: tuple[Measure, ...]
) -> str | None:
    if value is None:
        return None
    _require_keys(value, "curated", required={"partition_by_month"})
    partition_column = value["partition_by_month"]
    column = columns.get(partition_column)
    if column is None or column.type != "date":
        raise SchemaError(
            f"curated partition_by_month must name a date column: {partition_column!r}"
        )

    for name in [*columns, *(m.name for m in measures)]:
        if not _IDENTIFIER_RE.match(name):
            raise SchemaError(f"with curated output, {name!r} must be a plain identifier")
        if name in CURATED_RESERVED_NAMES:
            raise SchemaError(f"with curated output, {name!r} is a reserved name")
    return partition_column


def _parse_rate(value: Any) -> Decimal | None:
    if value is None:
        return None
    # bool is an int subclass in Python; `true` is not a rate.
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise SchemaError(f"max_rejection_rate must be a number from 0 to 1: {value!r}")
    rate = Decimal(str(value))
    if not 0 <= rate <= 1:
        raise SchemaError(f"max_rejection_rate must be a number from 0 to 1: {value!r}")
    return rate


def _parse_column(data: dict[str, Any]) -> Column:
    column_type = data.get("type")
    if column_type not in COLUMN_OPTIONS:
        raise SchemaError(f"column type must be one of {sorted(COLUMN_OPTIONS)}: {data!r}")
    options = COLUMN_OPTIONS[column_type]
    _require_keys(data, "column", required={"name", "type"}, optional=options)

    name = data["name"]
    if not isinstance(name, str) or not name or name != name.strip().lower():
        # Headers are matched after trimming and lowercasing, so a schema
        # column name must already be in that form to ever match.
        raise SchemaError(f"column name must be non-empty, trimmed lowercase: {name!r}")

    flags = {}
    for option in options & data.keys():
        if not isinstance(data[option], bool):
            raise SchemaError(f"column {name!r} option {option!r} must be true or false")
        flags[option] = data[option]
    return Column(name=name, type=column_type, **flags)


def _parse_measure(data: dict[str, Any], columns: dict[str, Column]) -> Measure:
    _require_keys(data, "measure", required={"name", "multiply"}, optional={"total"})
    name = data["name"]
    if not isinstance(name, str) or not name:
        raise SchemaError(f"measure name must be a non-empty string: {data!r}")

    multiply = tuple(_non_empty_list(data["multiply"], f"measure {name!r} multiply"))
    for column_name in multiply:
        column = columns.get(column_name)
        if column is None or column.type not in NUMERIC_TYPES:
            raise SchemaError(
                f"measure {name!r} can only multiply integer or decimal columns, "
                f"not {column_name!r}"
            )

    total = data.get("total", False)
    if not isinstance(total, bool):
        raise SchemaError(f"measure {name!r} option 'total' must be true or false")

    return Measure(
        name=name,
        multiply=multiply,
        total=total,
        is_decimal=any(columns[c].type == "decimal" for c in multiply),
    )


def _require_keys(
    data: Any, what: str, *, required: set[str], optional: frozenset[str] | set[str] = frozenset()
) -> None:
    if not isinstance(data, dict):
        raise SchemaError(f"{what} must be a JSON object")
    missing = required - data.keys()
    if missing:
        raise SchemaError(f"{what} is missing {sorted(missing)}")
    unknown = data.keys() - required - optional
    if unknown:
        raise SchemaError(f"{what} has unknown keys {sorted(unknown)}")


def _non_empty_list(value: Any, what: str) -> list[Any]:
    if not isinstance(value, list) or not value:
        raise SchemaError(f"{what} must be a non-empty list")
    return value

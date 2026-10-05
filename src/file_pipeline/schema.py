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
      }
    }

Column types and their options:

* `date`: `YYYY-MM-DD`, a real calendar date. Rejection: `invalid_<name>`.
* `string`: any text, trimmed. `required` rejects an empty value
  (`empty_<name>`).
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
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cache
from importlib import resources
from pathlib import Path
from typing import Any

DEFAULT_SCHEMA = "sales"

COLUMN_OPTIONS = {
    "date": frozenset(),
    "string": frozenset({"required"}),
    "integer": frozenset({"positive", "signed"}),
    "decimal": frozenset({"non_negative"}),
}
GROUPABLE_TYPES = frozenset({"date", "string"})
NUMERIC_TYPES = frozenset({"integer", "decimal"})


class SchemaError(Exception):
    """The schema file itself is invalid."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    required: bool = False
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
class Schema:
    name: str
    columns: tuple[Column, ...]
    group_by: str
    measures: tuple[Measure, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


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
    _require_keys(data, "schema", required={"name", "columns", "aggregation"})
    name = data["name"]
    if not isinstance(name, str) or not name:
        raise SchemaError("schema name must be a non-empty string")

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

    return Schema(name=name, columns=columns, group_by=group_by, measures=measures)


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

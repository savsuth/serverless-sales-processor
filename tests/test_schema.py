import copy
import csv
import io
import json

import pytest

from file_pipeline.local import EXIT_FILE_REJECTED, EXIT_OK, main
from file_pipeline.processor import build_summary_dict, process_csv
from file_pipeline.schema import SchemaError, load_schema, parse_schema

# A second, non-sales dataset: proves the engine is driven by the schema
# alone. It also exercises options the sales schema doesn't use: a signed
# integer, a decimal that may be negative, grouping by a date, and two
# grand totals.
INVENTORY_SCHEMA = {
    "name": "inventory",
    "columns": [
        {"name": "counted_on", "type": "date"},
        {"name": "sku", "type": "string", "required": True},
        {"name": "units", "type": "integer", "signed": True},
        {"name": "unit_cost", "type": "decimal", "non_negative": True},
        {"name": "adjustment", "type": "decimal"},
    ],
    "aggregation": {
        "group_by": "counted_on",
        "measures": [
            {"name": "units", "multiply": ["units"], "total": True},
            {"name": "stock_value", "multiply": ["units", "unit_cost"], "total": True},
            {"name": "adjustment", "multiply": ["adjustment"]},
        ],
    },
}

INVENTORY_CSV = """counted_on,sku,units,unit_cost,adjustment
2024-03-01,A-1,10,2.50,0
2024-03-01,B-2,-2,4.00,-1.25
2024-03-02,A-1,0,2.50,3
2024-03-02,,5,1.00,0
2024-03-02,C-3,abc,1.00,0
2024-03-02,C-3,1,-1.00,0
2024-03-02,C-3,1,1.00,NaN
"""


def _process(csv_text, schema):
    rejected = io.StringIO()
    result = process_csv(io.BytesIO(csv_text.encode()), rejected, schema=schema)
    rejected.seek(0)
    return result, list(csv.reader(rejected))


def test_bundled_sales_schema_loads():
    schema = load_schema()
    assert schema.name == "sales"
    assert schema.column_names == ("date", "product", "quantity", "unit_price")
    assert schema.group_by == "product"
    assert [(m.name, m.multiply, m.total) for m in schema.measures] == [
        ("quantity", ("quantity",), False),
        ("revenue", ("quantity", "unit_price"), True),
    ]


def test_unknown_bundled_schema_name_is_an_error():
    with pytest.raises(SchemaError):
        load_schema("does-not-exist")


def test_a_different_schema_drives_validation_and_totals():
    result, rejected_rows = _process(INVENTORY_CSV, parse_schema(INVENTORY_SCHEMA))

    assert build_summary_dict(result) == {
        "valid_row_count": 3,
        "rejected_row_count": 4,
        "by_counted_on": {
            "2024-03-01": {"units": 8, "stock_value": "17.00", "adjustment": "-1.25"},
            "2024-03-02": {"units": 0, "stock_value": "0.00", "adjustment": "3"},
        },
        "total_units": 8,
        "total_stock_value": "17.00",
    }
    assert [row[-1] for row in rejected_rows[1:]] == [
        "empty_sku",
        "invalid_units",
        "negative_unit_cost",
        "non_finite_adjustment",
    ]
    assert result.by_counted_on["2024-03-01"].units == 8
    assert result.total_units == 8


def test_cli_accepts_a_schema_file_path(tmp_path):
    schema_path = tmp_path / "inventory.json"
    schema_path.write_text(json.dumps(INVENTORY_SCHEMA))
    input_path = tmp_path / "inventory.csv"
    input_path.write_text(INVENTORY_CSV)
    out = tmp_path / "out"

    code = main(
        ["--input", str(input_path), "--output-dir", str(out), "--schema", str(schema_path)]
    )

    assert code == EXIT_OK
    assert json.loads((out / "summary.json").read_text())["total_units"] == 8


def test_cli_rejects_an_invalid_schema_without_writing_output(tmp_path):
    schema_path = tmp_path / "broken.json"
    schema_path.write_text(json.dumps({"name": "broken"}))
    input_path = tmp_path / "input.csv"
    input_path.write_text(INVENTORY_CSV)
    out = tmp_path / "out"

    code = main(
        ["--input", str(input_path), "--output-dir", str(out), "--schema", str(schema_path)]
    )

    assert code == EXIT_FILE_REJECTED
    assert not out.exists()


def _broken(change):
    schema = copy.deepcopy(INVENTORY_SCHEMA)
    change(schema)
    return schema


@pytest.mark.parametrize(
    "schema",
    [
        _broken(lambda s: s["columns"][0].update(type="timestamp")),
        _broken(lambda s: s["columns"][1].update(requird=True)),
        _broken(lambda s: s["columns"][3].update(positive=True)),
        _broken(lambda s: s["columns"][1].update(required="yes")),
        _broken(lambda s: s["columns"][1].update(name="SKU")),
        _broken(lambda s: s["columns"].append({"name": "sku", "type": "string"})),
        _broken(lambda s: s.update(columns=[])),
        _broken(lambda s: s["aggregation"].update(group_by="warehouse")),
        _broken(lambda s: s["aggregation"].update(group_by="units")),
        _broken(lambda s: s["aggregation"]["measures"][0].update(multiply=["sku"])),
        _broken(lambda s: s["aggregation"]["measures"][0].update(multiply=["missing"])),
        _broken(lambda s: s["aggregation"]["measures"][1].update(name="units")),
        _broken(lambda s: s["aggregation"]["measures"][0].update(total="yes")),
        _broken(lambda s: s.pop("aggregation")),
        _broken(lambda s: s.update(extra=1)),
    ],
    ids=[
        "unknown_type",
        "misspelled_option",
        "option_not_valid_for_type",
        "option_not_boolean",
        "column_name_not_lowercase",
        "duplicate_column",
        "no_columns",
        "group_by_unknown_column",
        "group_by_numeric_column",
        "measure_multiplies_string_column",
        "measure_multiplies_unknown_column",
        "duplicate_measure",
        "total_not_boolean",
        "missing_aggregation",
        "unknown_top_level_key",
    ],
)
def test_invalid_schemas_are_rejected(schema):
    with pytest.raises(SchemaError):
        parse_schema(schema)

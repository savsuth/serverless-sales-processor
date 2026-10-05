import gzip
import io
from decimal import Decimal

from file_pipeline.curated import CuratedWriter, athena_columns, read_curated_file
from file_pipeline.processor import process_csv
from file_pipeline.schema import load_schema, parse_schema

JOB_ID = "a" * 64

CSV = """date,product,quantity,unit_price
2024-01-31,Widget,3,9.99
2024-02-01,"Gadget, ""Deluxe""
edition",1,19.50
bad-date,Widget,1,1.00
2024-01-05,Café,2,0.10
"""


def _curated_files(csv_text, schema=None, job_id=JOB_ID):
    schema = schema or load_schema()
    with CuratedWriter(schema, job_id) as writer:
        process_csv(
            io.BytesIO(csv_text.encode("utf-8")),
            io.StringIO(),
            schema=schema,
            on_valid_row=writer.add,
        )
        return dict(writer.files())


def test_valid_rows_are_split_into_one_file_per_month():
    files = _curated_files(CSV)

    assert list(files) == [
        f"curated/sales/month=2024-01/{JOB_ID}.json.gz",
        f"curated/sales/month=2024-02/{JOB_ID}.json.gz",
    ]
    january = read_curated_file(files[f"curated/sales/month=2024-01/{JOB_ID}.json.gz"])
    assert january == [
        {
            "date": "2024-01-31",
            "product": "Widget",
            "quantity": 3,
            "unit_price": Decimal("9.99"),
            "revenue": Decimal("29.97"),
            "job_id": JOB_ID,
            "source_row_number": 1,
        },
        {
            "date": "2024-01-05",
            "product": "Café",
            "quantity": 2,
            "unit_price": Decimal("0.10"),
            "revenue": Decimal("0.20"),
            "job_id": JOB_ID,
            "source_row_number": 4,
        },
    ]


def test_text_with_commas_quotes_and_line_breaks_survives_intact():
    files = _curated_files(CSV)

    (february,) = read_curated_file(files[f"curated/sales/month=2024-02/{JOB_ID}.json.gz"])
    assert february["product"] == 'Gadget, "Deluxe"\nedition'


def test_each_line_is_one_json_object_with_plain_decimal_numbers():
    files = _curated_files(CSV)

    lines = gzip.decompress(files[f"curated/sales/month=2024-01/{JOB_ID}.json.gz"]).splitlines()
    assert lines[0] == (
        b'{"date":"2024-01-31","product":"Widget","quantity":3,"unit_price":9.99,'
        b'"revenue":29.97,"job_id":"' + JOB_ID.encode() + b'","source_row_number":1}'
    )


def test_output_bytes_are_deterministic_so_retries_overwrite_identically():
    assert _curated_files(CSV) == _curated_files(CSV)


def test_a_file_with_no_valid_rows_produces_no_curated_files():
    assert _curated_files("date,product,quantity,unit_price\nbad,Widget,1,1.00\n") == {}


def test_athena_columns_for_the_sales_schema():
    assert athena_columns(load_schema()) == [
        ("date", "date"),
        ("product", "string"),
        ("quantity", "bigint"),
        ("unit_price", "decimal(38,18)"),
        ("revenue", "decimal(38,18)"),
        ("job_id", "string"),
        ("source_row_number", "bigint"),
    ]


def test_measures_named_after_a_column_are_not_repeated():
    schema = parse_schema(
        {
            "name": "stock",
            "columns": [
                {"name": "counted_on", "type": "date"},
                {"name": "sku", "type": "string"},
                {"name": "units", "type": "integer"},
            ],
            "aggregation": {
                "group_by": "sku",
                "measures": [{"name": "units", "multiply": ["units"], "total": True}],
            },
            "curated": {"partition_by_month": "counted_on"},
        }
    )
    assert [name for name, _ in athena_columns(schema)] == [
        "counted_on",
        "sku",
        "units",
        "job_id",
        "source_row_number",
    ]


def test_curated_rows_keep_each_rows_own_spelling():
    csv_text = (
        "date,product,quantity,unit_price\n"
        "2024-01-01,Widget,1,1.00\n2024-01-02,wIdGeT,1,2.00\n"
    )
    files = _curated_files(csv_text)
    (body,) = files.values()
    assert [row["product"] for row in read_curated_file(body)] == ["Widget", "wIdGeT"]

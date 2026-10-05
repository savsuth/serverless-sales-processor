import csv
import dataclasses
import io
from decimal import Decimal

import pytest

from file_pipeline.processor import (
    InputTooLargeError,
    MalformedCSVError,
    build_summary_dict,
    process_csv,
)
from file_pipeline.schema import load_schema


def _run(csv_text: str, max_bytes: int | None = None):
    binary = io.BytesIO(csv_text.encode("utf-8"))
    rejected = io.StringIO()
    kwargs = {} if max_bytes is None else {"max_bytes": max_bytes}
    result = process_csv(binary, rejected, **kwargs)
    rejected.seek(0)
    rejected_rows = list(csv.reader(rejected))
    return result, rejected_rows


VALID_CSV = """date,product,quantity,unit_price
2024-01-05,Widget,3,9.99
2024-01-06,Gadget,1,19.50
2024-01-06,Widget,2,9.99
"""


def test_valid_csv_totals_and_status():
    result, rejected_rows = _run(VALID_CSV)

    assert result.status == "completed"
    assert result.valid_row_count == 4 - 1  # header excluded
    assert result.rejected_row_count == 0
    assert result.total_revenue == Decimal("9.99") * 3 + Decimal("19.50") + Decimal("9.99") * 2
    assert rejected_rows == [
        ["date", "product", "quantity", "unit_price", "row_number", "rejection_reason"]
    ]


def test_decimal_precision_no_float_error():
    # 0.1 + 0.2 style traps: with float arithmetic this would not equal
    # exactly 0.3 * 10 due to binary floating point representation.
    csv_text = "date,product,quantity,unit_price\n" + "".join(
        "2024-01-01,Widget,1,0.10\n" for _ in range(3)
    )
    result, _ = _run(csv_text)
    assert result.total_revenue == Decimal("0.30")
    assert build_summary_dict(result)["total_revenue"] == "0.30"


def test_by_product_aggregation_groups_and_sums():
    result, _ = _run(VALID_CSV)
    assert result.by_product["Widget"].quantity == 5
    assert result.by_product["Widget"].revenue == Decimal("9.99") * 3 + Decimal("9.99") * 2
    assert result.by_product["Gadget"].quantity == 1
    assert result.by_product["Gadget"].revenue == Decimal("19.50")


@pytest.mark.parametrize(
    "row,expected_reason",
    [
        ("2024-13-01,Widget,1,1.00", "invalid_date"),
        ("2024-02-30,Widget,1,1.00", "invalid_date"),
        ("not-a-date,Widget,1,1.00", "invalid_date"),
        ("2024-01-01,,1,1.00", "empty_product"),
        ("2024-01-01,Widget,0,1.00", "non_positive_quantity"),
        ("2024-01-01,Widget,-1,1.00", "invalid_quantity"),
        ("2024-01-01,Widget,1.5,1.00", "invalid_quantity"),
        ("2024-01-01,Widget,abc,1.00", "invalid_quantity"),
        ("2024-01-01,Widget,1,-1.00", "negative_unit_price"),
        ("2024-01-01,Widget,1,abc", "invalid_unit_price"),
        ("2024-01-01,Widget,1,NaN", "non_finite_unit_price"),
        ("2024-01-01,Widget,1,Infinity", "non_finite_unit_price"),
        ("2024-01-01,Widget,1,-Infinity", "non_finite_unit_price"),
        ("2024-01-01,Widget,1,1e10", "invalid_unit_price"),
    ],
)
def test_row_validation_reasons(row, expected_reason):
    csv_text = f"date,product,quantity,unit_price\n{row}\n"
    result, rejected_rows = _run(csv_text)
    assert result.valid_row_count == 0
    assert result.rejected_row_count == 1
    assert rejected_rows[1][-1] == expected_reason


def test_zero_valid_rows_is_validation_failed_but_still_produces_output():
    csv_text = "date,product,quantity,unit_price\nbad-date,Widget,1,1.00\n"
    result, rejected_rows = _run(csv_text)
    assert result.status == "validation_failed"
    assert result.rejected_row_count == 1
    assert len(rejected_rows) == 2  # header + one rejected data row


def test_row_length_mismatch_is_rejected_not_fatal():
    csv_text = "date,product,quantity,unit_price\n2024-01-01,Widget,1\n2024-01-02,Gadget,2,5.00\n"
    result, rejected_rows = _run(csv_text)
    assert result.valid_row_count == 1
    assert result.rejected_row_count == 1
    assert rejected_rows[1][-1] == "row_length_mismatch"


def test_missing_required_column_rejects_whole_file():
    csv_text = "date,product,quantity\n2024-01-01,Widget,1\n"
    with pytest.raises(MalformedCSVError) as exc_info:
        _run(csv_text)
    assert exc_info.value.code == "missing_required_columns"


def test_duplicate_headers_reject_whole_file():
    csv_text = "date,product,quantity,quantity,unit_price\n2024-01-01,Widget,1,1,1.00\n"
    with pytest.raises(MalformedCSVError) as exc_info:
        _run(csv_text)
    assert exc_info.value.code == "duplicate_header"


def test_empty_file_rejects_whole_file():
    with pytest.raises(MalformedCSVError) as exc_info:
        _run("")
    assert exc_info.value.code == "empty_file"


def test_header_only_no_data_rows_is_validation_failed():
    result, rejected_rows = _run("date,product,quantity,unit_price\n")
    assert result.status == "validation_failed"
    assert result.valid_row_count == 0
    assert result.rejected_row_count == 0
    assert rejected_rows == [
        ["date", "product", "quantity", "unit_price", "row_number", "rejection_reason"]
    ]


def test_header_matching_is_case_insensitive_and_trims_whitespace():
    csv_text = " Date , PRODUCT,Quantity ,unit_price\n2024-01-01,Widget,1,1.00\n"
    result, _ = _run(csv_text)
    assert result.status == "completed"
    assert result.valid_row_count == 1


def test_extra_columns_are_ignored_for_valid_rows():
    csv_text = "date,product,quantity,unit_price,warehouse\n2024-01-01,Widget,1,1.00,west\n"
    result, _ = _run(csv_text)
    assert result.status == "completed"
    assert result.valid_row_count == 1
    assert result.by_product["Widget"].quantity == 1


def test_extra_columns_are_preserved_verbatim_in_rejected_rows():
    csv_text = "date,product,quantity,unit_price,warehouse\nbad-date,Widget,1,1.00,west\n"
    _, rejected_rows = _run(csv_text)
    assert rejected_rows[0] == [
        "date",
        "product",
        "quantity",
        "unit_price",
        "warehouse",
        "row_number",
        "rejection_reason",
    ]
    assert rejected_rows[1] == ["bad-date", "Widget", "1", "1.00", "west", "1", "invalid_date"]


def test_bom_is_stripped():
    csv_text = "date,product,quantity,unit_price\n2024-01-01,Widget,1,1.00\n"
    binary = io.BytesIO(csv_text.encode("utf-8-sig"))
    rejected = io.StringIO()
    result = process_csv(binary, rejected)
    assert result.status == "completed"
    assert result.valid_row_count == 1


def test_input_too_large_is_rejected():
    csv_text = "date,product,quantity,unit_price\n" + "".join(
        "2024-01-01,Widget,1,1.00\n" for _ in range(1000)
    )
    with pytest.raises(InputTooLargeError):
        _run(csv_text, max_bytes=100)


def test_field_size_limit_raises_malformed_csv_error():
    huge_field = "x" * 200_000
    csv_text = f"date,product,quantity,unit_price\n2024-01-01,{huge_field},1,1.00\n"
    with pytest.raises(MalformedCSVError) as exc_info:
        _run(csv_text)
    assert exc_info.value.code == "csv_parse_error"


@pytest.mark.parametrize(
    "raw_value",
    ["=SUM(A1:A9)", "+1+1", "-2+3", "@cmd|calc", "\ttab-lead"],
)
def test_formula_injection_prefixes_are_neutralized_in_rejected_rows(raw_value):
    csv_text = f"date,product,quantity,unit_price\nbad-date,{raw_value},1,1.00\n"
    _, rejected_rows = _run(csv_text)
    product_cell = rejected_rows[1][1]
    assert product_cell == "'" + raw_value
    assert product_cell[0] == "'"


def test_blank_physical_lines_are_skipped_not_counted_as_rows():
    csv_text = "date,product,quantity,unit_price\n\n2024-01-01,Widget,1,1.00\n\n"
    result, _ = _run(csv_text)
    assert result.valid_row_count == 1
    assert result.rejected_row_count == 0


def test_summary_dict_serializes_money_as_plain_decimal_strings():
    result, _ = _run(VALID_CSV)
    summary = build_summary_dict(result)
    assert isinstance(summary["total_revenue"], str)
    for product_summary in summary["by_product"].values():
        assert isinstance(product_summary["revenue"], str)
        # no scientific notation, no float artifacts
        assert "e" not in product_summary["revenue"].lower()


# --- Rejection-rate threshold (the sales schema allows at most 50%) ---


def test_rejection_rate_at_the_limit_still_completes():
    csv_text = "date,product,quantity,unit_price\n2024-01-01,Widget,1,1.00\nbad,Widget,1,1.00\n"
    result, _ = _run(csv_text)
    assert result.status == "completed_with_rejections"
    assert result.error_code is None


def test_rejection_rate_above_the_limit_fails_but_keeps_totals():
    csv_text = (
        "date,product,quantity,unit_price\n"
        "2024-01-01,Widget,2,1.50\nbad,Widget,1,1.00\n2024-01-01,,1,1.00\n"
    )
    result, rejected_rows = _run(csv_text)
    assert result.status == "validation_failed"
    assert result.error_code == "rejection_rate_exceeded"
    assert result.error_message == "2 of 3 rows rejected (66.7%), above the 50% limit"
    assert build_summary_dict(result)["total_revenue"] == "3.00"
    assert len(rejected_rows) == 3


def test_zero_valid_rows_reports_no_valid_rows():
    result, _ = _run("date,product,quantity,unit_price\nbad-date,Widget,1,1.00\n")
    assert result.error_code == "no_valid_rows"


def test_schema_without_a_limit_accepts_any_rejection_rate():
    schema = dataclasses.replace(load_schema(), max_rejection_rate=None)
    csv_text = "date,product,quantity,unit_price\n2024-01-01,Widget,1,1.00\n" + "bad,W,1,1\n" * 9
    binary = io.BytesIO(csv_text.encode("utf-8"))
    result = process_csv(binary, io.StringIO(), schema=schema)
    assert result.status == "completed_with_rejections"

"""Property-based tests.

Hypothesis generates random sales files row by row, deciding for each
row whether it is valid or broken in one specific way. Because the
generator knows the right answer for every row, each test can compare
the processor's output against an independent calculation, or check a
rule that must hold for every possible file. When a test fails,
Hypothesis shrinks the input to the smallest file that still fails.
"""

from __future__ import annotations

import csv
import io
import json
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import boto3
from hypothesis import given, settings
from hypothesis import strategies as st
from moto import mock_aws

from file_pipeline import handler, jobs, notifications, storage
from file_pipeline.curated import CuratedWriter, read_curated_file
from file_pipeline.local import main
from file_pipeline.processor import build_summary_dict, process_csv, summary_json_bytes
from file_pipeline.schema import load_schema

HEADER = ["date", "product", "quantity", "unit_price"]
FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")
_REPORTS = ("summary.json", "rejected_rows.csv")
_INPUT_BUCKET = "prop-input"
_OUTPUT_BUCKET = "prop-output"


@dataclass(frozen=True)
class Row:
    fields: tuple[str, ...]
    reason: str | None = None  # expected rejection reason; None means valid
    product: str = ""
    quantity: int = 0
    unit_price: Decimal = Decimal("0")


BLANK = Row(fields=())  # a blank physical line, which the processor skips


# --- Generators ----------------------------------------------------------

_padding = st.sampled_from(["", " ", "  "])
_text = st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00")


def _padded(value: st.SearchStrategy[str]) -> st.SearchStrategy[str]:
    return st.tuples(_padding, value, _padding).map("".join)


@st.composite
def valid_rows(draw) -> Row:
    date = draw(st.dates()).isoformat()
    # Some names differ only in letter case: the sales schema groups them.
    product = draw(
        st.one_of(
            st.sampled_from(["Widget", "widget", " WIDGET ", "Gadget", "gadget"]),
            st.text(_text, min_size=1, max_size=12).filter(lambda s: s.strip()),
        )
    )
    quantity = draw(st.integers(min_value=1, max_value=10**6))
    leading_zeros = "0" * draw(st.integers(min_value=0, max_value=2))
    cents = draw(st.integers(min_value=0, max_value=10**9))
    scale = draw(st.integers(min_value=0, max_value=4))
    unit_price_text = format(Decimal(cents).scaleb(-scale), "f")

    fields = (
        draw(_padded(st.just(date))),
        product,
        draw(_padded(st.just(leading_zeros + str(quantity)))),
        draw(_padded(st.just(unit_price_text))),
    )
    return Row(
        fields=fields,
        product=product.strip(),
        quantity=quantity,
        unit_price=Decimal(unit_price_text),
    )


# (column index, bad value, expected reason). Each bad value breaks
# exactly one column of an otherwise valid row, so the expected reason is
# unambiguous.
_BREAKAGES = [
    *[(0, v, "invalid_date") for v in ("2024-02-30", "2024-13-01", "not-a-date", "", "2024/01/01")],
    *[(1, v, "empty_product") for v in ("", " ", "\t")],
    *[(2, v, "non_positive_quantity") for v in ("0", "00")],
    *[(2, v, "invalid_quantity") for v in ("-1", "1.5", "abc", "", "+1", "1e3")],
    *[(3, v, "negative_unit_price") for v in ("-1.00", "-0", "-5")],
    *[(3, v, "non_finite_unit_price") for v in ("NaN", "inf", "-Infinity", "+inf")],
    *[(3, v, "invalid_unit_price") for v in ("abc", "1e10", "", "1.", ".5", "+1.00", "1,000.00")],
]


@st.composite
def invalid_rows(draw) -> Row:
    base = draw(valid_rows())
    if draw(st.integers(min_value=0, max_value=9)) == 0:
        fields = base.fields[:-1] if draw(st.booleans()) else (*base.fields, "extra")
        return Row(fields=fields, reason="row_length_mismatch")

    column, bad_value, reason = draw(st.sampled_from(_BREAKAGES))
    fields = list(base.fields)
    fields[column] = bad_value
    return Row(fields=tuple(fields), reason=reason)


sales_files = st.lists(st.one_of(valid_rows(), invalid_rows(), st.just(BLANK)), max_size=40)


# --- Helpers ---------------------------------------------------------------


def _to_csv_bytes(rows: list[Row]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(HEADER)
    for row in rows:
        if row is BLANK:
            buffer.write("\r\n")
        else:
            writer.writerow(row.fields)
    return buffer.getvalue().encode("utf-8")


def _process(rows: list[Row]):
    rejected = io.StringIO(newline="")
    result = process_csv(io.BytesIO(_to_csv_bytes(rows)), rejected)
    return result, rejected.getvalue()


def _expected_status(valid_count: int, rejected_count: int) -> str:
    if valid_count == 0:
        return "validation_failed"
    # The sales schema fails a file when more than half its rows are rejected.
    if rejected_count / (valid_count + rejected_count) > 0.5:
        return "validation_failed"
    return "completed" if rejected_count == 0 else "completed_with_rejections"


def _expected_summary(rows: list[Row]) -> dict:
    valid = [r for r in rows if r is not BLANK and r.reason is None]
    rejected = [r for r in rows if r is not BLANK and r.reason is not None]
    # Product names are compared case-insensitively and reported under the
    # alphabetically first spelling.
    totals: dict[str, tuple[int, Decimal]] = {}
    names: dict[str, str] = {}
    total_revenue = Decimal("0")
    for row in valid:
        revenue = row.quantity * row.unit_price
        key = row.product.lower()
        names[key] = min(names.get(key, row.product), row.product)
        quantity_so_far, revenue_so_far = totals.get(key, (0, Decimal("0")))
        totals[key] = (quantity_so_far + row.quantity, revenue_so_far + revenue)
        total_revenue += revenue
    return {
        "total_revenue": format(total_revenue, "f"),
        "valid_row_count": len(valid),
        "rejected_row_count": len(rejected),
        "by_product": {
            names[key]: {"quantity": quantity, "revenue": format(revenue, "f")}
            for key, (quantity, revenue) in totals.items()
        },
    }


def _sanitized(value: str) -> str:
    return "'" + value if value.startswith(FORMULA_PREFIXES) else value


# --- Properties ------------------------------------------------------------


def _without_warnings(summary: dict) -> dict:
    return {k: v for k, v in summary.items() if k != "warnings"}


@settings(max_examples=300, deadline=None)
@given(sales_files)
def test_totals_match_an_independent_calculation(rows):
    result, _ = _process(rows)
    expected = _expected_summary(rows)

    assert _without_warnings(build_summary_dict(result)) == expected
    assert result.status == _expected_status(
        expected["valid_row_count"], expected["rejected_row_count"]
    )


@settings(max_examples=300, deadline=None)
@given(sales_files)
def test_product_revenues_add_up_to_total_revenue(rows):
    result, _ = _process(rows)

    assert sum((t.revenue for t in result.by_product.values()), Decimal("0")) == (
        result.total_revenue
    )


@settings(max_examples=300, deadline=None)
@given(sales_files)
def test_every_non_blank_row_is_either_counted_or_rejected(rows):
    result, _ = _process(rows)

    assert result.valid_row_count + result.rejected_row_count == sum(
        1 for r in rows if r is not BLANK
    )


@settings(max_examples=200, deadline=None)
@given(sales_files, st.data())
def test_row_order_does_not_change_the_summary(rows, data):
    shuffled = data.draw(st.permutations(rows))

    original, _ = _process(rows)
    reordered, _ = _process(shuffled)

    # Warnings name row numbers, which do move with the rows; their counts
    # must not.
    assert _without_warnings(build_summary_dict(reordered)) == _without_warnings(
        build_summary_dict(original)
    )
    assert {k: w["count"] for k, w in reordered.warnings.items()} == {
        k: w["count"] for k, w in original.warnings.items()
    }


@settings(max_examples=300, deadline=None)
@given(sales_files)
def test_rejected_rows_report_lists_each_rejection_with_its_reason(rows):
    _, rejected_csv = _process(rows)

    expected = [[*HEADER, "row_number", "rejection_reason"]]
    for row_number, row in enumerate(rows, start=1):
        if row is not BLANK and row.reason is not None:
            expected.append([*(_sanitized(f) for f in row.fields), str(row_number), row.reason])

    assert list(csv.reader(io.StringIO(rejected_csv, newline=""))) == expected


@settings(max_examples=25, deadline=None)
@given(sales_files)
def test_local_cli_and_lambda_write_identical_bytes(rows):
    csv_bytes = _to_csv_bytes(rows)

    with tempfile.TemporaryDirectory() as tmp:
        input_path = Path(tmp) / "input.csv"
        input_path.write_bytes(csv_bytes)
        out = Path(tmp) / "out"
        main(["--input", str(input_path), "--output-dir", str(out)])
        local_reports = {name: (out / name).read_bytes() for name in _REPORTS}

    assert _run_lambda(csv_bytes) == local_reports


def _run_lambda(csv_bytes: bytes) -> dict[str, bytes]:
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=_INPUT_BUCKET)
        s3.put_bucket_versioning(
            Bucket=_INPUT_BUCKET, VersioningConfiguration={"Status": "Enabled"}
        )
        s3.create_bucket(Bucket=_OUTPUT_BUCKET)
        table = boto3.resource("dynamodb", region_name="us-east-1").create_table(
            TableName="jobs",
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        sns = boto3.client("sns", region_name="us-east-1")
        deps = handler.Dependencies(
            job_store=jobs.JobStore(table),
            object_storage=storage.S3Storage(s3),
            notifier=notifications.SNSNotifier(sns, sns.create_topic(Name="t")["TopicArn"]),
            output_bucket=_OUTPUT_BUCKET,
        )

        version_id = s3.put_object(Bucket=_INPUT_BUCKET, Key="input.csv", Body=csv_bytes)[
            "VersionId"
        ]
        s3_event = {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": _INPUT_BUCKET},
                        "object": {"key": "input.csv", "versionId": version_id},
                    },
                }
            ]
        }
        event = {"Records": [{"messageId": "m1", "body": json.dumps(s3_event)}]}
        assert handler.handle_event(event, deps) == {"batchItemFailures": []}

        job_id = jobs.compute_job_id(_INPUT_BUCKET, "input.csv", version_id)
        return {
            name: s3.get_object(Bucket=_OUTPUT_BUCKET, Key=f"reports/{job_id}/{name}")[
                "Body"
            ].read()
            for name in _REPORTS
        }


@settings(max_examples=200, deadline=None)
@given(sales_files)
def test_curated_rows_add_up_to_the_summary(rows):
    """What Athena adds up over the curated data must equal the report."""
    with CuratedWriter(load_schema(), "job") as writer:
        result = process_csv(
            io.BytesIO(_to_csv_bytes(rows)), io.StringIO(), on_valid_row=writer.add
        )
        files = list(writer.files())

    curated_rows = [(key, row) for key, body in files for row in read_curated_file(body)]
    assert len(curated_rows) == result.valid_row_count
    assert sum((row["revenue"] for _, row in curated_rows), Decimal("0")) == result.total_revenue
    for key, row in curated_rows:
        assert f"/month={row['date'][:7]}/" in key


def _to_csv_bytes_with(rows: list[Row], delimiter: str) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, delimiter=delimiter)
    writer.writerow(HEADER)
    for row in rows:
        if row is BLANK:
            buffer.write("\r\n")
        else:
            writer.writerow(row.fields)
    return buffer.getvalue().encode("utf-8")


@settings(max_examples=200, deadline=None)
@given(sales_files, st.sampled_from([";", "\t"]))
def test_the_field_separator_does_not_change_the_result(rows, delimiter):
    comma = process_csv(io.BytesIO(_to_csv_bytes(rows)), io.StringIO())
    other = process_csv(io.BytesIO(_to_csv_bytes_with(rows, delimiter)), io.StringIO())

    assert summary_json_bytes(other) == summary_json_bytes(comma)
    assert other.status == comma.status


@settings(max_examples=200, deadline=None)
@given(sales_files)
def test_grouping_curated_rows_like_the_athena_queries_reproduces_the_report(rows):
    """The saved queries use GROUP BY lower(product) with min(product)."""
    with CuratedWriter(load_schema(), "job") as writer:
        result = process_csv(
            io.BytesIO(_to_csv_bytes(rows)), io.StringIO(), on_valid_row=writer.add
        )
        curated_rows = [row for _, body in writer.files() for row in read_curated_file(body)]

    groups: dict[str, list] = {}
    for row in curated_rows:
        name, quantity, revenue = groups.get(row["product"].lower(), [row["product"], 0, 0])
        groups[row["product"].lower()] = [
            min(name, row["product"]),
            quantity + row["quantity"],
            revenue + row["revenue"],
        ]
    by_product = {
        name: {"quantity": quantity, "revenue": format(Decimal(revenue), "f")}
        for name, quantity, revenue in groups.values()
    }
    assert by_product == build_summary_dict(result)["by_product"]


@settings(max_examples=200, deadline=None)
@given(sales_files)
def test_repeated_row_count_equals_valid_rows_minus_distinct_rows(rows):
    result, _ = _process(rows)
    distinct = {
        (
            r.fields[0].strip(),
            r.product.lower(),
            r.quantity,
            r.unit_price.normalize(),
        )
        for r in rows
        if r is not BLANK and r.reason is None
    }
    assert result.warnings["duplicate_rows"]["count"] == result.valid_row_count - len(distinct)

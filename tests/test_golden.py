"""Byte-for-byte snapshot tests for every file in samples/.

Each sample is run through the local CLI and through the Lambda handler
(against moto), and both must reproduce the stored outputs under
tests/golden/<sample>/ exactly. Any change to an output byte -- a
reordered key, a different rounding, a new column -- fails here, which
is what makes refactoring processor.py safe.

When an output change is intended, regenerate the snapshots with

    UPDATE_GOLDEN=1 pytest tests/test_golden.py

and review the diff under tests/golden/ before committing it.
"""

import json
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from file_pipeline import handler, jobs, notifications, storage
from file_pipeline.local import main

SAMPLES = Path(__file__).parent.parent / "samples"
GOLDEN = Path(__file__).parent / "golden"
REPORT_FILES = ("summary.json", "rejected_rows.csv")
UPDATE = os.environ.get("UPDATE_GOLDEN") == "1"

SAMPLE_PATHS = sorted(SAMPLES.glob("*.csv"))

INPUT_BUCKET = "golden-input"
OUTPUT_BUCKET = "golden-output"
REGION = "us-east-1"


def _golden_reports(stem: str) -> dict[str, bytes]:
    golden_dir = GOLDEN / stem
    return {
        name: (golden_dir / name).read_bytes()
        for name in REPORT_FILES
        if (golden_dir / name).exists()
    }


def _golden_exit_code(stem: str) -> int:
    return int((GOLDEN / stem / "exit_code").read_text().strip())


def _write_golden(stem: str, exit_code: int, reports: dict[str, bytes]) -> None:
    golden_dir = GOLDEN / stem
    golden_dir.mkdir(parents=True, exist_ok=True)
    for name in REPORT_FILES:
        (golden_dir / name).unlink(missing_ok=True)
    for name, content in reports.items():
        (golden_dir / name).write_bytes(content)
    (golden_dir / "exit_code").write_text(f"{exit_code}\n")


def test_every_sample_has_a_snapshot():
    if UPDATE:
        pytest.skip("snapshots are being regenerated")
    assert SAMPLE_PATHS
    assert {p.stem for p in SAMPLE_PATHS} == {p.name for p in GOLDEN.iterdir() if p.is_dir()}


@pytest.mark.parametrize("sample", SAMPLE_PATHS, ids=lambda p: p.stem)
def test_local_cli_output_matches_snapshot(sample, tmp_path):
    out = tmp_path / "out"
    exit_code = main(["--input", str(sample), "--output-dir", str(out)])
    reports = {name: (out / name).read_bytes() for name in REPORT_FILES if (out / name).exists()}

    if UPDATE:
        _write_golden(sample.stem, exit_code, reports)
        return

    assert exit_code == _golden_exit_code(sample.stem)
    assert reports == _golden_reports(sample.stem)


@pytest.fixture
def moto_stack():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=INPUT_BUCKET)
        s3.put_bucket_versioning(
            Bucket=INPUT_BUCKET, VersioningConfiguration={"Status": "Enabled"}
        )
        s3.create_bucket(Bucket=OUTPUT_BUCKET)

        table = boto3.resource("dynamodb", region_name=REGION).create_table(
            TableName="golden-jobs",
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        sns = boto3.client("sns", region_name=REGION)
        topic_arn = sns.create_topic(Name="golden-notifications")["TopicArn"]

        deps = handler.Dependencies(
            job_store=jobs.JobStore(table),
            object_storage=storage.S3Storage(s3),
            notifier=notifications.SNSNotifier(sns, topic_arn),
            output_bucket=OUTPUT_BUCKET,
        )
        yield s3, table, deps


@pytest.mark.parametrize("sample", SAMPLE_PATHS, ids=lambda p: p.stem)
def test_lambda_output_matches_snapshot(sample, moto_stack):
    if UPDATE:
        pytest.skip("snapshots are being regenerated")
    s3, table, deps = moto_stack
    key = sample.name
    version_id = s3.put_object(Bucket=INPUT_BUCKET, Key=key, Body=sample.read_bytes())[
        "VersionId"
    ]
    event = {
        "Records": [
            {
                "messageId": "m1",
                "body": json.dumps(
                    {
                        "Records": [
                            {
                                "eventName": "ObjectCreated:Put",
                                "s3": {
                                    "bucket": {"name": INPUT_BUCKET},
                                    "object": {"key": key, "versionId": version_id},
                                },
                            }
                        ]
                    }
                ),
            }
        ]
    }

    assert handler.handle_event(event, deps) == {"batchItemFailures": []}

    job_id = jobs.compute_job_id(INPUT_BUCKET, key, version_id)
    job = table.get_item(Key={"job_id": job_id})["Item"]
    expected_reports = _golden_reports(sample.stem)

    if not expected_reports:
        assert job["status"] == "validation_failed"
        assert "output_summary_key" not in job
        return

    actual_reports = {
        name: s3.get_object(Bucket=OUTPUT_BUCKET, Key=f"reports/{job_id}/{name}")["Body"].read()
        for name in REPORT_FILES
    }
    assert actual_reports == expected_reports

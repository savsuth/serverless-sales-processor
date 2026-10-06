"""Reprocessing and job listing (src/file_pipeline/admin.py), against moto."""

import boto3
import pytest
from moto import mock_aws

from file_pipeline import admin, handler, jobs, notifications, storage

REGION = "us-east-1"
INPUT_BUCKET = "admin-input"
OUTPUT_BUCKET = "admin-output"
CSV = b"date,product,quantity,unit_price\n2024-01-05,Widget,3,9.99\n"


@pytest.fixture
def stack():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=INPUT_BUCKET)
        s3.put_bucket_versioning(Bucket=INPUT_BUCKET, VersioningConfiguration={"Status": "Enabled"})
        s3.create_bucket(Bucket=OUTPUT_BUCKET)
        table = boto3.resource("dynamodb", region_name=REGION).create_table(
            TableName="admin-jobs",
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "job_id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": admin.STATUS_INDEX,
                    "KeySchema": [
                        {"AttributeName": "status", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        sqs = boto3.client("sqs", region_name=REGION)
        queue_url = sqs.create_queue(QueueName="admin-queue")["QueueUrl"]
        sns = boto3.client("sns", region_name=REGION)
        deps = handler.Dependencies(
            job_store=jobs.JobStore(table, rules=("sales", 1)),
            object_storage=storage.S3Storage(s3),
            notifier=notifications.SNSNotifier(sns, sns.create_topic(Name="t")["TopicArn"]),
            output_bucket=OUTPUT_BUCKET,
        )
        yield {"s3": s3, "table": table, "sqs": sqs, "queue_url": queue_url, "deps": deps}


def _process(stack, key, body=CSV):
    version_id = stack["s3"].put_object(Bucket=INPUT_BUCKET, Key=key, Body=body)["VersionId"]
    message = admin.s3_event_message(INPUT_BUCKET, key, version_id)
    handler.handle_event({"Records": [{"messageId": "m1", "body": message}]}, stack["deps"])
    return jobs.compute_job_id(INPUT_BUCKET, key, version_id)


def _deliver_queued(stack):
    messages = (
        stack["sqs"]
        .receive_message(QueueUrl=stack["queue_url"], MaxNumberOfMessages=10)
        .get("Messages", [])
    )
    records = [{"messageId": m["MessageId"], "body": m["Body"]} for m in messages]
    return handler.handle_event({"Records": records}, stack["deps"])


def test_event_message_round_trips_keys_with_spaces_and_plus_signs(stack):
    job_id = _process(stack, "Q1 sales+returns/Jan 2024.csv")
    assert stack["table"].get_item(Key={"job_id": job_id})["Item"]["status"] == "completed"


def test_reprocess_reruns_a_finished_job_without_duplicating_outputs(stack):
    job_id = _process(stack, "sales.csv")
    job = stack["table"].get_item(Key={"job_id": job_id})["Item"]
    curated_before = stack["s3"].list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix="curated/")

    assert admin.request_reprocess(stack["table"], stack["sqs"], stack["queue_url"], job)
    assert stack["table"].get_item(Key={"job_id": job_id})["Item"]["error_code"] == (
        "reprocess_requested"
    )
    assert _deliver_queued(stack) == {"batchItemFailures": []}

    rerun = stack["table"].get_item(Key={"job_id": job_id})["Item"]
    assert rerun["status"] == "completed"
    assert int(rerun["attempt_count"]) == 2
    assert "error_code" not in rerun
    curated_after = stack["s3"].list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix="curated/")
    assert curated_after["KeyCount"] == curated_before["KeyCount"] == 1


def test_reprocess_skips_a_job_that_is_running_or_changed(stack):
    job_id = _process(stack, "sales.csv")
    job = stack["table"].get_item(Key={"job_id": job_id})["Item"]

    running = {**job, "status": jobs.STATUS_PROCESSING}
    assert not admin.request_reprocess(stack["table"], stack["sqs"], stack["queue_url"], running)
    stale = {**job, "status": jobs.STATUS_DEAD_LETTERED}  # record says completed
    assert not admin.request_reprocess(stack["table"], stack["sqs"], stack["queue_url"], stale)
    assert "Messages" not in stack["sqs"].receive_message(QueueUrl=stack["queue_url"])


def test_jobs_with_status_lists_newest_first_and_never_fingerprints(stack):
    first = _process(stack, "a.csv")
    second = _process(stack, "b.csv", CSV.replace(b"Widget", b"Gadget"))

    listed = [job["job_id"] for job in admin.jobs_with_status(stack["table"], "completed")]
    assert listed == [second, first]
    assert all(not job_id.startswith("content#") for job_id in listed)


def test_processed_before_version():
    assert admin.processed_before_version({"schema_version": 1}, 2)
    assert not admin.processed_before_version({"schema_version": 2}, 2)
    assert admin.processed_before_version({}, 1)  # before versions were recorded


def test_upload_exists_until_its_version_is_deleted(stack):
    job_id = _process(stack, "sales.csv")
    job = stack["table"].get_item(Key={"job_id": job_id})["Item"]
    assert admin.upload_exists(stack["s3"], job)

    # What an expiring lifecycle rule does in the end: the version is gone.
    stack["s3"].delete_object(
        Bucket=INPUT_BUCKET, Key=job["source_key"], VersionId=job["source_version_id"]
    )
    assert not admin.upload_exists(stack["s3"], job)


def test_upload_behind_a_delete_marker_still_exists(stack):
    job_id = _process(stack, "sales.csv")
    job = stack["table"].get_item(Key={"job_id": job_id})["Item"]
    stack["s3"].delete_object(Bucket=INPUT_BUCKET, Key=job["source_key"])  # delete marker only

    # The version itself is still stored, so it can still be reprocessed.
    assert admin.upload_exists(stack["s3"], job)

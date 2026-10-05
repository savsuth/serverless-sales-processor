"""Integration tests for the Lambda handler against moto's in-memory AWS
emulation -- no real AWS credentials or network calls are used anywhere
in this file (moto intercepts every boto3 call)."""

import gzip
import hashlib
import json
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

from file_pipeline import handler, jobs, notifications, storage
from file_pipeline.curated import read_curated_file

TABLE_NAME = "test-jobs"
INPUT_BUCKET = "test-input-bucket"
OUTPUT_BUCKET = "test-output-bucket"
TOPIC_NAME = "test-notifications"
REGION = "us-east-1"

VALID_CSV = b"date,product,quantity,unit_price\n2024-01-05,Widget,3,9.99\n"
MIXED_CSV = b"date,product,quantity,unit_price\n2024-01-05,Widget,3,9.99\nbad-date,X,1,1.00\n"
ALL_INVALID_CSV = b"date,product,quantity,unit_price\nbad-date,Widget,1,1.00\n"
MALFORMED_CSV = b"date,product,quantity,quantity,unit_price\n2024-01-05,Widget,3,3,9.99\n"


class FakeNotifier:
    """Duck-types notifications.SNSNotifier's publish() so tests can
    control and observe outbound notifications without real SNS."""

    def __init__(self, fail_times: int = 0):
        self.calls: list[tuple[str, str, str]] = []
        self._fail_times = fail_times

    def publish(self, *, job_id: str, subject: str, body: str) -> None:
        self.calls.append((job_id, subject, body))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("simulated SNS failure")


class FlakyObjectStorage:
    """Wraps a real S3Storage and raises on demand, to simulate transient
    AWS errors independently of what moto itself does."""

    def __init__(self, inner: storage.S3Storage, *, fail_head=False, fail_get=False):
        self._inner = inner
        self.fail_head = fail_head
        self.fail_get = fail_get

    def head_object(self, *args, **kwargs):
        if self.fail_head:
            raise RuntimeError("simulated transient head_object error")
        return self._inner.head_object(*args, **kwargs)

    def open_object(self, *args, **kwargs):
        if self.fail_get:
            raise RuntimeError("simulated transient get_object error")
        return self._inner.open_object(*args, **kwargs)

    def put_report(self, *args, **kwargs):
        return self._inner.put_report(*args, **kwargs)


class FailNthPutS3Client:
    """Wraps a real boto3 S3 client and raises on the Nth put_object call,
    to simulate a crash partway through writing the two report objects."""

    def __init__(self, real_client, fail_on_call: int):
        self._real = real_client
        self._fail_on_call = fail_on_call
        self.put_object_calls = 0

    def __getattr__(self, name):
        return getattr(self._real, name)

    def put_object(self, **kwargs):
        self.put_object_calls += 1
        if self.put_object_calls == self._fail_on_call:
            raise RuntimeError("simulated failure partway through report write")
        return self._real.put_object(**kwargs)


@pytest.fixture
def aws_stack():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=INPUT_BUCKET)
        s3.put_bucket_versioning(
            Bucket=INPUT_BUCKET, VersioningConfiguration={"Status": "Enabled"}
        )
        s3.create_bucket(Bucket=OUTPUT_BUCKET)

        dynamodb = boto3.resource("dynamodb", region_name=REGION)
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        sns = boto3.client("sns", region_name=REGION)
        topic_arn = sns.create_topic(Name=TOPIC_NAME)["TopicArn"]

        yield {"s3": s3, "table": table, "sns": sns, "topic_arn": topic_arn}


def make_deps(aws_stack, *, notifier=None, object_storage=None, lease_seconds=300):
    return handler.Dependencies(
        job_store=jobs.JobStore(aws_stack["table"], lease_seconds=lease_seconds),
        object_storage=object_storage or storage.S3Storage(aws_stack["s3"]),
        notifier=notifier
        or notifications.SNSNotifier(aws_stack["sns"], aws_stack["topic_arn"]),
        output_bucket=OUTPUT_BUCKET,
    )


def upload(s3, key: str, body: bytes, bucket: str = INPUT_BUCKET) -> str:
    return s3.put_object(Bucket=bucket, Key=key, Body=body)["VersionId"]


def s3_event(bucket: str, raw_key: str, version_id: str, event_name="ObjectCreated:Put") -> dict:
    return {
        "Records": [
            {
                "eventName": event_name,
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": raw_key, "versionId": version_id},
                },
            }
        ]
    }


def sqs_record(body: dict, message_id: str = "m1") -> dict:
    return {"messageId": message_id, "body": json.dumps(body)}


def get_job(aws_stack, job_id: str) -> dict:
    return aws_stack["table"].get_item(Key={"job_id": job_id})["Item"]


def test_valid_csv_end_to_end(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    deps = make_deps(aws_stack)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    result = handler.handle_event(event, deps)

    assert result == {"batchItemFailures": []}

    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    job = get_job(aws_stack, job_id)
    assert job["status"] == "completed"
    assert job["notification_status"] == "sent"
    assert int(job["valid_row_count"]) == 1

    summary_obj = aws_stack["s3"].get_object(
        Bucket=OUTPUT_BUCKET, Key=f"reports/{job_id}/summary.json"
    )
    summary = json.loads(summary_obj["Body"].read())
    assert summary["total_revenue"] == "29.97"


def test_s3_test_event_is_ignored(aws_stack):
    deps = make_deps(aws_stack)
    event = {
        "Records": [sqs_record({"Service": "Amazon S3", "Event": "s3:TestEvent"})]
    }
    result = handler.handle_event(event, deps)
    assert result == {"batchItemFailures": []}


def test_duplicate_delivery_does_not_reprocess(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    notifier = FakeNotifier()
    deps = make_deps(aws_stack, notifier=notifier)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    handler.handle_event(event, deps)
    second_event = {
        "Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id), "m2")]
    }
    handler.handle_event(second_event, deps)

    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    job = get_job(aws_stack, job_id)
    assert int(job["attempt_count"]) == 1
    assert len(notifier.calls) == 1  # not resent: notification_status was already "sent"


def test_concurrent_duplicate_with_active_claim_stays_retryable(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    deps = make_deps(aws_stack)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)

    # Simulate another worker holding a live claim (never finalized).
    deps.job_store.claim(job_id, INPUT_BUCKET, "sales.csv", version_id)

    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}
    result = handler.handle_event(event, deps)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    job = get_job(aws_stack, job_id)
    assert job["status"] == "processing"


def test_recovery_after_expired_lease(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    deps = make_deps(aws_stack)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)

    # Simulate a crashed worker: claimed, never finalized, lease already expired.
    deps.job_store.claim(job_id, INPUT_BUCKET, "sales.csv", version_id)
    aws_stack["table"].update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET lease_expires_at = :past",
        ExpressionAttributeValues={":past": 0},
    )

    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}
    result = handler.handle_event(event, deps)

    assert result == {"batchItemFailures": []}
    job = get_job(aws_stack, job_id)
    assert job["status"] == "completed"
    assert int(job["attempt_count"]) == 2


def test_temporary_s3_error_is_returned_as_batch_item_failure(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    flaky = FlakyObjectStorage(storage.S3Storage(aws_stack["s3"]), fail_head=True)
    deps = make_deps(aws_stack, object_storage=flaky)

    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}
    result = handler.handle_event(event, deps)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    job = get_job(aws_stack, job_id)
    assert job["status"] == "failed"
    assert job["error_code"] == "s3_head_object_error"


def test_retry_after_partial_output_write_overwrites_safely(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)

    failing_client = FailNthPutS3Client(aws_stack["s3"], fail_on_call=2)
    failing_deps = make_deps(
        aws_stack, object_storage=storage.S3Storage(failing_client)
    )
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    first_result = handler.handle_event(event, failing_deps)
    assert first_result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert get_job(aws_stack, job_id)["status"] == "failed"
    # summary.json was written before the simulated crash on the 2nd put.
    aws_stack["s3"].get_object(Bucket=OUTPUT_BUCKET, Key=f"reports/{job_id}/summary.json")

    working_deps = make_deps(aws_stack)
    second_result = handler.handle_event(event, working_deps)
    assert second_result == {"batchItemFailures": []}

    job = get_job(aws_stack, job_id)
    assert job["status"] == "completed"
    rejected_obj = aws_stack["s3"].get_object(
        Bucket=OUTPUT_BUCKET, Key=f"reports/{job_id}/rejected_rows.csv"
    )
    assert rejected_obj["Body"].read().startswith(b"date,product,quantity,unit_price")


def test_sns_failure_after_processing_completed_retries_notification_only(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)

    failing_notifier = FakeNotifier(fail_times=1)
    deps = make_deps(aws_stack, notifier=failing_notifier)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    first_result = handler.handle_event(event, deps)
    assert first_result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}

    job = get_job(aws_stack, job_id)
    assert job["status"] == "completed"  # processing succeeded despite SNS failure
    assert job["notification_status"] == "failed"

    working_notifier = FakeNotifier()
    deps2 = make_deps(aws_stack, notifier=working_notifier)
    second_result = handler.handle_event(event, deps2)

    assert second_result == {"batchItemFailures": []}
    assert len(working_notifier.calls) == 1
    assert get_job(aws_stack, job_id)["notification_status"] == "sent"


def test_object_key_with_spaces_and_escaped_characters(aws_stack):
    real_key = "sales reports/Q1 2024.csv"
    version_id = upload(aws_stack["s3"], real_key, VALID_CSV)
    deps = make_deps(aws_stack)

    # S3 event notifications URL-encode the key with spaces as '+'.
    raw_event_key = "sales+reports/Q1+2024.csv"
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, raw_event_key, version_id))]}

    result = handler.handle_event(event, deps)

    assert result == {"batchItemFailures": []}
    job_id = jobs.compute_job_id(INPUT_BUCKET, real_key, version_id)
    job = get_job(aws_stack, job_id)
    assert job["status"] == "completed"
    assert job["source_key"] == real_key


def test_retrieves_the_specific_uploaded_object_version(aws_stack):
    v1 = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    v2 = upload(aws_stack["s3"], "sales.csv", MIXED_CSV)
    assert v1 != v2

    deps = make_deps(aws_stack)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", v1))]}
    handler.handle_event(event, deps)

    job_id_v1 = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", v1)
    summary = json.loads(
        aws_stack["s3"]
        .get_object(Bucket=OUTPUT_BUCKET, Key=f"reports/{job_id_v1}/summary.json")["Body"]
        .read()
    )
    assert summary["rejected_row_count"] == 0  # v1 has no invalid rows


def test_malformed_csv_end_to_end_is_validation_failed_with_no_reports(aws_stack):
    version_id = upload(aws_stack["s3"], "bad.csv", MALFORMED_CSV)
    notifier = FakeNotifier()
    deps = make_deps(aws_stack, notifier=notifier)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "bad.csv", version_id))]}

    result = handler.handle_event(event, deps)

    assert result == {"batchItemFailures": []}
    job_id = jobs.compute_job_id(INPUT_BUCKET, "bad.csv", version_id)
    job = get_job(aws_stack, job_id)
    assert job["status"] == "validation_failed"
    assert job["error_code"] == "duplicate_header"
    assert "output_summary_key" not in job
    assert len(notifier.calls) == 1


def test_oversized_object_is_rejected_before_download(aws_stack):
    version_id = upload(aws_stack["s3"], "big.csv", VALID_CSV)
    deps = make_deps(aws_stack)
    deps.max_input_bytes = 5  # smaller than the uploaded object

    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "big.csv", version_id))]}
    result = handler.handle_event(event, deps)

    assert result == {"batchItemFailures": []}
    job_id = jobs.compute_job_id(INPUT_BUCKET, "big.csv", version_id)
    job = get_job(aws_stack, job_id)
    assert job["status"] == "validation_failed"
    assert job["error_code"] == "input_too_large"


# --- Review additions: lease ownership, steal races, notification retries ---


class Crash(BaseException):
    """Simulates the Lambda process dying (not an Exception subclass, so
    the handler's own error handling cannot swallow it)."""


class HookedPutStorage:
    def __init__(self, inner, before_put=None, after_put=None):
        self._inner = inner
        self._before_put = before_put
        self._after_put = after_put

    def head_object(self, *a, **k):
        return self._inner.head_object(*a, **k)

    def open_object(self, *a, **k):
        return self._inner.open_object(*a, **k)

    def put_report(self, *a, **k):
        if self._before_put:
            self._before_put()
        result = self._inner.put_report(*a, **k)
        if self._after_put:
            self._after_put()
        return result


def expire_lease(aws_stack, job_id):
    aws_stack["table"].update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET lease_expires_at = :past",
        ExpressionAttributeValues={":past": 0},
    )


def test_stale_worker_cannot_overwrite_a_newer_workers_result(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    notifier = FakeNotifier()
    stealer = jobs.JobStore(aws_stack["table"])
    stolen = {}

    def steal_and_finish():
        expire_lease(aws_stack, job_id)
        claim = stealer.claim(job_id, INPUT_BUCKET, "sales.csv", version_id)
        assert claim.status is jobs.ClaimStatus.OWNED
        stolen["token"] = claim.lease_token
        assert stealer.finalize_success(
            job_id,
            claim.lease_token,
            status="completed",
            output_summary_key="reports/x/summary.json",
            output_rejected_key="reports/x/rejected_rows.csv",
            valid_row_count=99,
            rejected_row_count=0,
        )

    slow = HookedPutStorage(storage.S3Storage(aws_stack["s3"]), before_put=steal_and_finish)
    deps = make_deps(aws_stack, notifier=notifier, object_storage=slow)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    result = handler.handle_event(event, deps)

    assert result == {"batchItemFailures": []}
    job = get_job(aws_stack, job_id)
    assert int(job["valid_row_count"]) == 99
    assert job["lease_owner"] == stolen["token"]
    assert notifier.calls == []


def test_stale_worker_failure_marker_cannot_clobber_completed_job(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    old = jobs.JobStore(aws_stack["table"])
    new = jobs.JobStore(aws_stack["table"])

    old_claim = old.claim(job_id, INPUT_BUCKET, "sales.csv", version_id)
    expire_lease(aws_stack, job_id)
    new_claim = new.claim(job_id, INPUT_BUCKET, "sales.csv", version_id)
    new.finalize_success(
        job_id,
        new_claim.lease_token,
        status="completed",
        output_summary_key="a",
        output_rejected_key="b",
        valid_row_count=1,
        rejected_row_count=0,
    )

    stale = old_claim.lease_token
    assert old.mark_failed(job_id, stale, error_code="x", error_message="y") is False
    assert old.mark_notification_result(job_id, stale, sent=True) is False
    assert get_job(aws_stack, job_id)["status"] == "completed"


def test_only_one_of_two_racing_workers_steals_an_expired_lease(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    crashed = jobs.JobStore(aws_stack["table"])
    worker_a = jobs.JobStore(aws_stack["table"])
    worker_b = jobs.JobStore(aws_stack["table"])

    crashed.claim(job_id, INPUT_BUCKET, "sales.csv", version_id)
    expire_lease(aws_stack, job_id)

    # Worker B read the expired record before worker A stole it.
    stale_record = worker_b.get(job_id)
    worker_b.get = lambda _job_id: stale_record

    claim_a = worker_a.claim(job_id, INPUT_BUCKET, "sales.csv", version_id)
    claim_b = worker_b.claim(job_id, INPUT_BUCKET, "sales.csv", version_id)

    assert claim_a.status is jobs.ClaimStatus.OWNED
    assert claim_b.status is jobs.ClaimStatus.LOST_RACE
    assert get_job(aws_stack, job_id)["lease_owner"] == claim_a.lease_token


def test_notification_retry_for_validation_failed_job_does_not_reprocess(aws_stack):
    version_id = upload(aws_stack["s3"], "bad.csv", MALFORMED_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "bad.csv", version_id)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "bad.csv", version_id))]}

    first = handler.handle_event(event, make_deps(aws_stack, notifier=FakeNotifier(fail_times=1)))
    assert first == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    job = get_job(aws_stack, job_id)
    assert job["status"] == "validation_failed"
    assert job["notification_status"] == "failed"

    # Any attempt to touch S3 on the retry would raise; it must not try.
    no_s3 = FlakyObjectStorage(
        storage.S3Storage(aws_stack["s3"]), fail_head=True, fail_get=True
    )
    notifier = FakeNotifier()
    retry_deps = make_deps(aws_stack, notifier=notifier, object_storage=no_s3)
    second = handler.handle_event(event, retry_deps)

    assert second == {"batchItemFailures": []}
    assert len(notifier.calls) == 1
    job = get_job(aws_stack, job_id)
    assert job["notification_status"] == "sent"
    assert int(job["attempt_count"]) == 1


def test_crash_after_reports_written_but_before_finalize_recovers_after_lease_expiry(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    def die():
        raise Crash

    crashing = HookedPutStorage(storage.S3Storage(aws_stack["s3"]), after_put=die)
    with pytest.raises(Crash):
        handler.handle_event(event, make_deps(aws_stack, object_storage=crashing))

    assert get_job(aws_stack, job_id)["status"] == "processing"

    # While the dead worker's lease is still live, a duplicate must not take over...
    live = handler.handle_event(event, make_deps(aws_stack))
    assert live == {"batchItemFailures": [{"itemIdentifier": "m1"}]}

    # ...but once it expires, the retry finishes the job.
    expire_lease(aws_stack, job_id)
    recovered = handler.handle_event(event, make_deps(aws_stack))
    assert recovered == {"batchItemFailures": []}
    job = get_job(aws_stack, job_id)
    assert job["status"] == "completed"
    assert int(job["attempt_count"]) == 2


def test_unexpected_processing_error_never_stores_raw_exception_text(aws_stack, monkeypatch):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)

    def boom(*args, **kwargs):
        raise ValueError("customer row 4111-1111-1111-1111 exploded")

    monkeypatch.setattr(handler, "process_csv", boom)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    result = handler.handle_event(event, make_deps(aws_stack))

    # A bug in processing would recur on every retry, so the message is
    # acknowledged and the job parked as dead_lettered (see below).
    assert result == {"batchItemFailures": []}
    job = get_job(aws_stack, job_id)
    assert job["status"] == "dead_lettered"
    assert job["error_code"] == "processing_error"
    assert job["error_message"] == "ValueError"


def test_logs_are_pure_json_and_never_contain_csv_contents(aws_stack, capsys):
    version_id = upload(aws_stack["s3"], "sales.csv", MIXED_CSV)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    handler.handle_event(event, make_deps(aws_stack, notifier=FakeNotifier()))

    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    assert lines
    parsed = [json.loads(ln) for ln in lines]
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    assert any(entry.get("job_id") == job_id for entry in parsed)
    assert all({"level", "message"} <= entry.keys() for entry in parsed)
    joined = "\n".join(lines)
    assert "Widget" not in joined and "9.99" not in joined


# --- Rejection-rate threshold ---

HIGH_REJECTION_CSV = (
    b"date,product,quantity,unit_price\n"
    b"2024-01-05,Widget,3,9.99\nbad-date,X,1,1.00\n2024-01-06,,1,1.00\n"
)


def test_too_many_rejected_rows_fails_the_job_but_writes_reports(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", HIGH_REJECTION_CSV)
    notifier = FakeNotifier()
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    result = handler.handle_event(event, make_deps(aws_stack, notifier=notifier))

    assert result == {"batchItemFailures": []}
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    job = get_job(aws_stack, job_id)
    assert job["status"] == "validation_failed"
    assert job["error_code"] == "rejection_rate_exceeded"
    assert job["error_message"] == "2 of 3 rows rejected (66.7%), above the 50% limit"
    assert job["output_summary_key"] == f"reports/{job_id}/summary.json"
    (_, _, body), = notifier.calls
    assert "Error code: rejection_rate_exceeded" in body
    assert "Summary report: s3://" in body


def test_completed_job_has_no_error_code(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", MIXED_CSV)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    handler.handle_event(event, make_deps(aws_stack, notifier=FakeNotifier()))

    job = get_job(aws_stack, jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id))
    assert job["status"] == "completed_with_rejections"
    assert "error_code" not in job


# --- Duplicate content ---


def _upload_and_process(aws_stack, key, body, *, notifier=None):
    version_id = upload(aws_stack["s3"], key, body)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, key, version_id))]}
    result = handler.handle_event(event, make_deps(aws_stack, notifier=notifier or FakeNotifier()))
    return result, jobs.compute_job_id(INPUT_BUCKET, key, version_id)


def test_same_content_under_a_new_name_is_a_duplicate(aws_stack):
    _, first_id = _upload_and_process(aws_stack, "jan.csv", VALID_CSV)
    notifier = FakeNotifier()
    result, second_id = _upload_and_process(aws_stack, "jan-copy.csv", VALID_CSV, notifier=notifier)

    assert result == {"batchItemFailures": []}
    second = get_job(aws_stack, second_id)
    assert second["status"] == "duplicate_content"
    assert second["duplicate_of"] == first_id
    assert "output_summary_key" not in second
    reports = aws_stack["s3"].list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix=f"reports/{second_id}/")
    assert reports["KeyCount"] == 0
    ((_, subject, body),) = notifier.calls
    assert subject.startswith("CSV job duplicate_content")
    assert f"Duplicate of job: {first_id}" in body


def test_same_content_uploaded_again_under_the_same_name_is_a_duplicate(aws_stack):
    _, first_id = _upload_and_process(aws_stack, "sales.csv", VALID_CSV)
    _, second_id = _upload_and_process(aws_stack, "sales.csv", VALID_CSV)

    assert second_id != first_id  # a new S3 version is a new job...
    assert get_job(aws_stack, second_id)["status"] == "duplicate_content"  # ...but not new data


def test_duplicate_waits_while_the_original_job_is_unfinished(aws_stack, capsys):
    # The original job claimed the content, then its worker died.
    v1 = upload(aws_stack["s3"], "a.csv", VALID_CSV)
    first_id = jobs.compute_job_id(INPUT_BUCKET, "a.csv", v1)
    store = jobs.JobStore(aws_stack["table"])
    store.claim(first_id, INPUT_BUCKET, "a.csv", v1)
    store.claim_content(hashlib.sha256(VALID_CSV).hexdigest(), first_id)

    result, second_id = _upload_and_process(aws_stack, "b.csv", VALID_CSV)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    waiting = get_job(aws_stack, second_id)
    assert waiting["status"] == "failed"
    assert waiting["error_code"] == "waiting_for_original_job"
    assert '"level": "ERROR"' not in capsys.readouterr().out  # waiting is not an error

    # The original's own retry still owns the content and completes...
    expire_lease(aws_stack, first_id)
    event_a = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "a.csv", v1))]}
    assert handler.handle_event(event_a, make_deps(aws_stack, notifier=FakeNotifier())) == {
        "batchItemFailures": []
    }
    assert get_job(aws_stack, first_id)["status"] == "completed"

    # ...after which the waiting job's retry ends as a duplicate.
    v2 = waiting["source_version_id"]
    event_b = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "b.csv", v2))]}
    assert handler.handle_event(event_b, make_deps(aws_stack, notifier=FakeNotifier())) == {
        "batchItemFailures": []
    }
    assert get_job(aws_stack, second_id)["status"] == "duplicate_content"


def test_files_that_fail_validation_are_never_flagged_as_duplicates(aws_stack):
    _, first_id = _upload_and_process(aws_stack, "bad1.csv", ALL_INVALID_CSV)
    _, second_id = _upload_and_process(aws_stack, "bad2.csv", ALL_INVALID_CSV)

    for job_id in (first_id, second_id):
        job = get_job(aws_stack, job_id)
        assert job["status"] == "validation_failed"
        assert "output_summary_key" in job


def test_completed_job_records_its_content_fingerprint(aws_stack):
    _, job_id = _upload_and_process(aws_stack, "sales.csv", VALID_CSV)

    digest = hashlib.sha256(VALID_CSV).hexdigest()
    assert get_job(aws_stack, job_id)["content_sha256"] == digest
    assert get_job(aws_stack, f"content#{digest}")["first_job_id"] == job_id


def test_duplicate_notification_is_retried_without_reprocessing(aws_stack):
    _upload_and_process(aws_stack, "a.csv", VALID_CSV)
    v2 = upload(aws_stack["s3"], "b.csv", VALID_CSV)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "b.csv", v2))]}

    first = handler.handle_event(event, make_deps(aws_stack, notifier=FakeNotifier(fail_times=1)))
    assert first == {"batchItemFailures": [{"itemIdentifier": "m1"}]}

    notifier = FakeNotifier()
    no_s3 = FlakyObjectStorage(storage.S3Storage(aws_stack["s3"]), fail_head=True, fail_get=True)
    second = handler.handle_event(
        event, make_deps(aws_stack, notifier=notifier, object_storage=no_s3)
    )

    assert second == {"batchItemFailures": []}
    ((_, _, body),) = notifier.calls
    assert "Duplicate of job:" in body


# --- Curated output for Athena ---


def _curated_keys(aws_stack):
    listing = aws_stack["s3"].list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix="curated/")
    return sorted(obj["Key"] for obj in listing.get("Contents", []))


def test_completed_job_writes_its_valid_rows_for_athena(aws_stack):
    _, job_id = _upload_and_process(aws_stack, "sales.csv", MIXED_CSV)

    key = f"curated/sales/month=2024-01/{job_id}.json.gz"
    assert _curated_keys(aws_stack) == [key]
    body = aws_stack["s3"].get_object(Bucket=OUTPUT_BUCKET, Key=key)["Body"].read()
    (row,) = read_curated_file(body)
    assert row["revenue"] == Decimal("29.97")
    assert row["job_id"] == job_id
    assert row["source_row_number"] == 1


def test_failed_and_duplicate_jobs_write_no_rows_for_athena(aws_stack):
    _, first_id = _upload_and_process(aws_stack, "jan.csv", VALID_CSV)
    _upload_and_process(aws_stack, "jan-copy.csv", VALID_CSV)  # duplicate_content
    _upload_and_process(aws_stack, "mostly-bad.csv", HIGH_REJECTION_CSV)  # validation_failed
    _upload_and_process(aws_stack, "all-bad.csv", ALL_INVALID_CSV)  # validation_failed

    assert _curated_keys(aws_stack) == [f"curated/sales/month=2024-01/{first_id}.json.gz"]


# --- Which objects are inputs ---


@pytest.mark.parametrize(
    "key,expected",
    [
        ("sales.csv", True),
        ("SALES.CSV", True),
        ("Q1/Sales.Csv", True),
        ("sales.csv.gz", True),
        ("SALES.CSV.GZ", True),
        ("notes.txt", False),
        ("sales.csv.bak", False),
        ("sales.gz", False),
    ],
)
def test_input_keys_are_matched_case_insensitively(key, expected):
    assert storage.is_input_key(key) is expected


def test_uppercase_extension_is_processed(aws_stack):
    _, job_id = _upload_and_process(aws_stack, "SALES.CSV", VALID_CSV)
    assert get_job(aws_stack, job_id)["status"] == "completed"


def test_non_csv_object_is_ignored_without_a_job_record(aws_stack):
    result, job_id = _upload_and_process(aws_stack, "notes.txt", b"not a csv")
    assert result == {"batchItemFailures": []}
    assert "Item" not in aws_stack["table"].get_item(Key={"job_id": job_id})


def test_gzip_upload_is_processed_and_counts_as_the_same_content(aws_stack):
    _, plain_id = _upload_and_process(aws_stack, "sales.csv", VALID_CSV)
    _, gz_id = _upload_and_process(aws_stack, "sales.csv.gz", gzip.compress(VALID_CSV))

    gz_job = get_job(aws_stack, gz_id)
    assert gz_job["status"] == "duplicate_content"
    assert gz_job["duplicate_of"] == plain_id


def test_warning_counts_reach_the_job_record_and_the_notification(aws_stack):
    body = b"date,product,quantity,unit_price\n2024-01-01,Widget,1,9.99\n2024-01-01,Widget,1,9.99\n"
    notifier = FakeNotifier()
    _, job_id = _upload_and_process(aws_stack, "repeats.csv", body, notifier=notifier)

    job = get_job(aws_stack, job_id)
    assert job["status"] == "completed"
    assert {k: int(v) for k, v in job["warning_counts"].items()} == {
        "duplicate_rows": 1,
        "outliers": 0,
    }
    ((_, _, message),) = notifier.calls
    assert "Warnings: 1 repeated row (row numbers are in summary.json)" in message


# --- When retrying stops ---


def sqs_record_on_delivery(body: dict, receive_count: int, message_id: str = "m1") -> dict:
    record = sqs_record(body, message_id)
    record["attributes"] = {"ApproximateReceiveCount": str(receive_count)}
    return record


def test_transient_error_before_the_last_delivery_stays_retryable(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    flaky = FlakyObjectStorage(storage.S3Storage(aws_stack["s3"]), fail_get=True)
    event = {
        "Records": [sqs_record_on_delivery(s3_event(INPUT_BUCKET, "sales.csv", version_id), 4)]
    }

    result = handler.handle_event(event, make_deps(aws_stack, object_storage=flaky))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    job = get_job(aws_stack, jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id))
    assert job["status"] == "failed"


def test_transient_error_on_the_last_delivery_is_dead_lettered_and_redrive_resumes(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    flaky = FlakyObjectStorage(storage.S3Storage(aws_stack["s3"]), fail_get=True)
    notifier = FakeNotifier()
    s3_body = s3_event(INPUT_BUCKET, "sales.csv", version_id)

    last = handler.handle_event(
        {"Records": [sqs_record_on_delivery(s3_body, 5)]},
        make_deps(aws_stack, notifier=notifier, object_storage=flaky),
    )

    assert last == {"batchItemFailures": [{"itemIdentifier": "m1"}]}  # SQS moves it to the DLQ
    job = get_job(aws_stack, job_id)
    assert job["status"] == "dead_lettered"
    assert job["error_code"] == "s3_get_object_error"
    ((_, subject, body),) = notifier.calls
    assert subject.startswith("CSV job dead_lettered")
    assert "scripts/redrive_dlq.sh" in body

    # A redrive delivers the message again with a fresh receive count.
    redriven = handler.handle_event(
        {"Records": [sqs_record_on_delivery(s3_body, 1)]}, make_deps(aws_stack)
    )
    assert redriven == {"batchItemFailures": []}
    assert get_job(aws_stack, job_id)["status"] == "completed"


def test_processing_bug_is_parked_at_once_with_reprocess_instructions(aws_stack, monkeypatch):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    monkeypatch.setattr(handler, "process_csv", lambda *a, **k: 1 / 0)
    notifier = FakeNotifier()

    result = handler.handle_event(
        {"Records": [sqs_record_on_delivery(s3_event(INPUT_BUCKET, "sales.csv", version_id), 1)]},
        make_deps(aws_stack, notifier=notifier),
    )

    assert result == {"batchItemFailures": []}  # no pointless retries
    assert get_job(aws_stack, job_id)["status"] == "dead_lettered"
    ((_, _, body),) = notifier.calls
    assert f"scripts/reprocess.py --job-id {job_id}" in body


@pytest.mark.parametrize("error", [OSError("connection reset"), ConnectionError("eof")])
def test_network_error_while_reading_is_retried_not_parked(aws_stack, monkeypatch, error):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)

    def fail_reading(*args, **kwargs):
        raise error

    monkeypatch.setattr(handler, "process_csv", fail_reading)
    result = handler.handle_event(
        {"Records": [sqs_record_on_delivery(s3_event(INPUT_BUCKET, "sales.csv", version_id), 1)]},
        make_deps(aws_stack),
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    job = get_job(aws_stack, jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id))
    assert (job["status"], job["error_code"]) == ("failed", "s3_read_error")


def test_waiting_duplicate_is_dead_lettered_on_its_last_delivery(aws_stack):
    v1 = upload(aws_stack["s3"], "a.csv", VALID_CSV)
    first_id = jobs.compute_job_id(INPUT_BUCKET, "a.csv", v1)
    store = jobs.JobStore(aws_stack["table"])
    store.claim(first_id, INPUT_BUCKET, "a.csv", v1)
    store.claim_content(hashlib.sha256(VALID_CSV).hexdigest(), first_id)

    v2 = upload(aws_stack["s3"], "b.csv", VALID_CSV)
    result = handler.handle_event(
        {"Records": [sqs_record_on_delivery(s3_event(INPUT_BUCKET, "b.csv", v2), 5)]},
        make_deps(aws_stack, notifier=FakeNotifier()),
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    job = get_job(aws_stack, jobs.compute_job_id(INPUT_BUCKET, "b.csv", v2))
    assert (job["status"], job["error_code"]) == ("dead_lettered", "waiting_for_original_job")


# --- manifest.json: the completion marker ---


class RecordingS3Client:
    def __init__(self, real_client):
        self._real = real_client
        self.put_keys: list[str] = []

    def __getattr__(self, name):
        return getattr(self._real, name)

    def put_object(self, **kwargs):
        self.put_keys.append(kwargs["Key"])
        return self._real.put_object(**kwargs)


def test_manifest_is_written_last_and_matches_every_output(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", MIXED_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    recorder = RecordingS3Client(aws_stack["s3"])
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}

    handler.handle_event(
        event, make_deps(aws_stack, object_storage=storage.S3Storage(recorder))
    )

    assert recorder.put_keys[-1] == f"reports/{job_id}/manifest.json"
    manifest = json.loads(
        aws_stack["s3"]
        .get_object(Bucket=OUTPUT_BUCKET, Key=f"reports/{job_id}/manifest.json")["Body"]
        .read()
    )
    assert manifest["job_id"] == job_id
    assert manifest["status"] == "completed_with_rejections"
    assert (manifest["schema"], manifest["schema_version"]) == ("sales", 1)
    assert manifest["content_sha256"] == hashlib.sha256(MIXED_CSV).hexdigest()
    listed = manifest["files"] + manifest["curated_files"]
    assert [entry["key"] for entry in listed] == recorder.put_keys[:-1]
    for entry in listed:
        body = aws_stack["s3"].get_object(Bucket=OUTPUT_BUCKET, Key=entry["key"])["Body"].read()
        assert entry == {
            "key": entry["key"],
            "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
        }


def test_failed_validation_manifest_lists_no_curated_files(aws_stack):
    _, job_id = _upload_and_process(aws_stack, "bad.csv", ALL_INVALID_CSV)
    manifest = json.loads(
        aws_stack["s3"]
        .get_object(Bucket=OUTPUT_BUCKET, Key=f"reports/{job_id}/manifest.json")["Body"]
        .read()
    )
    assert (manifest["status"], manifest["error_code"]) == ("validation_failed", "no_valid_rows")
    assert manifest["curated_files"] == []


def test_claims_record_the_rules_version_they_ran_with(aws_stack):
    store = jobs.JobStore(aws_stack["table"], rules=("sales", 3))
    store.claim("job-1", INPUT_BUCKET, "a.csv", "v1")
    job = get_job(aws_stack, "job-1")
    assert (job["schema_name"], int(job["schema_version"])) == ("sales", 3)

    store.mark_failed("job-1", job["lease_owner"], error_code="x", error_message="y")
    newer = jobs.JobStore(aws_stack["table"], rules=("sales", 4))
    assert newer.claim("job-1", INPUT_BUCKET, "a.csv", "v1").status is jobs.ClaimStatus.OWNED
    assert int(get_job(aws_stack, "job-1")["schema_version"]) == 4


# --- Metrics (CloudWatch Embedded Metric Format) ---


def _metric_lines(capsys):
    lines = [json.loads(ln) for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    return [ln for ln in lines if ln.get("message") == "job_attempt_metrics"]


def test_each_attempt_emits_one_metrics_line_with_its_outcome(aws_stack, capsys):
    _, job_id = _upload_and_process(aws_stack, "sales.csv", MIXED_CSV)

    (line,) = _metric_lines(capsys)
    assert line["job_id"] == job_id
    assert line["Status"] == "completed_with_rejections"
    assert (line["JobAttempts"], line["ValidRows"], line["RejectedRows"]) == (1, 1, 1)
    assert isinstance(line["ProcessingMs"], int)
    (directive,) = line["_aws"]["CloudWatchMetrics"]
    assert directive["Namespace"] == "CsvSalesPipeline"
    assert directive["Dimensions"] == [["Status"]]
    assert {m["Name"] for m in directive["Metrics"]} == {
        "JobAttempts",
        "ValidRows",
        "RejectedRows",
        "ProcessingMs",
    }


def test_failed_and_duplicate_attempts_are_counted_by_status(aws_stack, capsys):
    version_id = upload(aws_stack["s3"], "a.csv", VALID_CSV)
    flaky = FlakyObjectStorage(storage.S3Storage(aws_stack["s3"]), fail_get=True)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "a.csv", version_id))]}
    handler.handle_event(event, make_deps(aws_stack, object_storage=flaky))
    handler.handle_event(event, make_deps(aws_stack, notifier=FakeNotifier()))
    _upload_and_process(aws_stack, "copy.csv", VALID_CSV)

    assert [line["Status"] for line in _metric_lines(capsys)] == [
        "failed",
        "completed",
        "duplicate_content",
    ]


def test_no_metrics_when_another_worker_has_taken_over(aws_stack, capsys):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)

    def steal():
        expire_lease(aws_stack, job_id)
        jobs.JobStore(aws_stack["table"]).claim(job_id, INPUT_BUCKET, "sales.csv", version_id)

    slow = HookedPutStorage(storage.S3Storage(aws_stack["s3"]), before_put=steal)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}
    handler.handle_event(event, make_deps(aws_stack, object_storage=slow))

    assert _metric_lines(capsys) == []


# --- EventBridge events ---


class FakeEvents:
    def __init__(self, fail_times: int = 0):
        self.events: list[tuple[str, dict]] = []
        self._fail_times = fail_times

    def publish(self, *, detail_type, detail):
        self.events.append((detail_type, detail))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("simulated EventBridge failure")


def test_finished_job_publishes_an_event_with_its_source_file(aws_stack):
    version_id = upload(aws_stack["s3"], "q1/sales.csv", MIXED_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "q1/sales.csv", version_id)
    deps = make_deps(aws_stack, notifier=FakeNotifier())
    deps.events = FakeEvents()

    handler.handle_event(
        {"Records": [sqs_record(s3_event(INPUT_BUCKET, "q1/sales.csv", version_id))]}, deps
    )

    ((detail_type, detail),) = deps.events.events
    assert detail_type == "CSV job finished"
    assert detail == {
        "job_id": job_id,
        "status": "completed_with_rejections",
        "valid_row_count": 1,
        "rejected_row_count": 1,
        "error_code": None,
        "duplicate_of": None,
        "warning_counts": {"duplicate_rows": 0, "outliers": 0},
        "output_bucket": OUTPUT_BUCKET,
        "summary_key": f"reports/{job_id}/summary.json",
        "rejected_key": f"reports/{job_id}/rejected_rows.csv",
        "source_bucket": INPUT_BUCKET,
        "source_key": "q1/sales.csv",
        "source_version_id": version_id,
    }


def test_event_failure_is_retried_with_the_notification(aws_stack):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    job_id = jobs.compute_job_id(INPUT_BUCKET, "sales.csv", version_id)
    event = {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}
    deps = make_deps(aws_stack, notifier=FakeNotifier())
    deps.events = FakeEvents(fail_times=1)

    assert handler.handle_event(event, deps) == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert get_job(aws_stack, job_id)["notification_status"] == "failed"
    assert handler.handle_event(event, deps) == {"batchItemFailures": []}
    assert [d["status"] for _, d in deps.events.events] == ["completed", "completed"]
    assert get_job(aws_stack, job_id)["notification_status"] == "sent"


def test_dead_lettered_job_publishes_an_event(aws_stack, monkeypatch):
    version_id = upload(aws_stack["s3"], "sales.csv", VALID_CSV)
    monkeypatch.setattr(handler, "process_csv", lambda *a, **k: 1 / 0)
    deps = make_deps(aws_stack, notifier=FakeNotifier())
    deps.events = FakeEvents()

    handler.handle_event(
        {"Records": [sqs_record(s3_event(INPUT_BUCKET, "sales.csv", version_id))]}, deps
    )

    ((detail_type, detail),) = deps.events.events
    assert detail_type == "CSV job dead-lettered"
    assert (detail["error_code"], detail["in_dead_letter_queue"]) == ("processing_error", False)


def test_event_publisher_sends_to_eventbridge(aws_stack):
    events_client = boto3.client("events", region_name=REGION)
    publisher = notifications.EventPublisher(events_client, "default", "csv-sales-pipeline")
    publisher.publish(detail_type="CSV job finished", detail={"job_id": "x", "status": "completed"})

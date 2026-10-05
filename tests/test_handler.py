"""Integration tests for the Lambda handler against moto's in-memory AWS
emulation -- no real AWS credentials or network calls are used anywhere
in this file (moto intercepts every boto3 call)."""

import json

import boto3
import pytest
from moto import mock_aws

from file_pipeline import handler, jobs, notifications, storage

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

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    job = get_job(aws_stack, job_id)
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

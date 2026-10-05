"""SQS-triggered Lambda entry point.

Wires processor.py + storage.py + jobs.py + notifications.py together.
`lambda_handler` is a thin wrapper: all real logic lives in `handle_event`,
which takes an explicit `Dependencies` bundle so tests can inject
moto-backed or fake clients instead of talking to real AWS.

Return contract: this function returns SQS's partial-batch-failure shape
(`{"batchItemFailures": [{"itemIdentifier": messageId}, ...]}`), which
requires `FunctionResponseTypes: ["ReportBatchItemFailures"]` on the SQS
event source mapping (see infra/). Any messageId NOT listed is deleted
from the queue by Lambda; anything listed becomes visible again after the
visibility timeout and is retried (or moves to the DLQ once
maxReceiveCount is exhausted -- see jobs.py's docstring and README's
retry/redrive section for how to recognize and recover that).
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
import tempfile
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

from botocore.exceptions import ClientError

from file_pipeline import jobs, notifications, storage
from file_pipeline.processor import (
    MAX_INPUT_BYTES,
    InputTooLargeError,
    MalformedCSVError,
    process_csv,
    summary_json_bytes,
)
from file_pipeline.schema import DEFAULT_SCHEMA, Schema, load_schema


class _StdoutHandler(logging.StreamHandler):
    """Writes to whatever sys.stdout currently is (so tests can capture
    it). Lambda ships stdout to CloudWatch Logs unmodified."""

    def __init__(self) -> None:
        super().__init__(sys.stdout)

    @property
    def stream(self):  # type: ignore[override]
        return sys.stdout

    @stream.setter
    def stream(self, value) -> None:
        pass


# The Lambda Python runtime prefixes root-logger output with a timestamp
# and request ID, which stops CloudWatch Logs Insights from auto-parsing
# the JSON. A dedicated non-propagating handler keeps each line pure JSON.
logger = logging.getLogger("file_pipeline")
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
logger.propagate = False
if not logger.handlers:
    _handler = _StdoutHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)

REJECTED_BUFFER_SPOOL_BYTES = 1024 * 1024


def _describe_exception(exc: Exception) -> str:
    """A safe, content-free description for the job record and logs. Raw
    exception text is never stored: an unexpected error raised while
    parsing could quote a fragment of the CSV."""
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        return f"AWS error {code} during {exc.operation_name}"
    return type(exc).__name__


def _log(level: int, message: str, **fields: Any) -> None:
    # Structured JSON logs carrying job_id / error_code for grep-ability
    # in CloudWatch Logs Insights. Never pass row content or credentials
    # into `fields` -- only identifiers, statuses, and error codes.
    payload = {"level": logging.getLevelName(level), "message": message, **fields}
    logger.log(level, json.dumps(payload, default=str))


def _log_info(message: str, **fields: Any) -> None:
    _log(logging.INFO, message, **fields)


def _log_error(message: str, **fields: Any) -> None:
    _log(logging.ERROR, message, **fields)


@dataclass
class Dependencies:
    job_store: jobs.JobStore
    object_storage: storage.S3Storage
    notifier: notifications.SNSNotifier
    output_bucket: str
    max_input_bytes: int = MAX_INPUT_BYTES
    schema: Schema = field(default_factory=load_schema)


def _default_dependencies() -> Dependencies:
    import boto3

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["JOB_TABLE_NAME"])
    lease_seconds = int(os.environ.get("LEASE_SECONDS", jobs.DEFAULT_LEASE_SECONDS))

    return Dependencies(
        job_store=jobs.JobStore(table, lease_seconds=lease_seconds),
        object_storage=storage.S3Storage(boto3.client("s3")),
        notifier=notifications.SNSNotifier(
            boto3.client("sns"), os.environ["NOTIFICATION_TOPIC_ARN"]
        ),
        output_bucket=os.environ["OUTPUT_BUCKET_NAME"],
        max_input_bytes=int(os.environ.get("MAX_INPUT_BYTES", MAX_INPUT_BYTES)),
        schema=load_schema(os.environ.get("SCHEMA_NAME", DEFAULT_SCHEMA)),
    )


def decode_s3_event_key(raw_key: str) -> str:
    """S3 event notification object keys are URL-encoded with spaces as
    '+' (form-encoding style), not '%20' -- unquote_plus is the correct
    decoder. A literal '+' in a real key is itself percent-encoded by S3
    as %2B, so this never mis-decodes an intentional plus sign."""
    return urllib.parse.unquote_plus(raw_key)


def lambda_handler(event: dict, context: Any) -> dict:
    return handle_event(event, _default_dependencies())


def handle_event(event: dict, deps: Dependencies) -> dict:
    batch_item_failures: list[dict[str, str]] = []

    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")
        try:
            ok = _process_sqs_record(record, deps, message_id=message_id)
        except Exception as exc:  # noqa: BLE001 - last-resort safety net
            _log_error(
                "unhandled_error_processing_sqs_record",
                sqs_message_id=message_id,
                error=type(exc).__name__,
            )
            ok = False
        if not ok:
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}


def _process_sqs_record(record: dict, deps: Dependencies, *, message_id: str) -> bool:
    try:
        body = json.loads(record["body"])
    except (KeyError, json.JSONDecodeError) as exc:
        _log_error("invalid_sqs_message_body", sqs_message_id=message_id, error=str(exc))
        return False

    if body.get("Event") == "s3:TestEvent":
        _log_info("s3_test_event_ignored", sqs_message_id=message_id)
        return True

    s3_records = body.get("Records")
    if not s3_records:
        _log_info("sqs_message_with_no_s3_records_ignored", sqs_message_id=message_id)
        return True

    all_ok = True
    for s3_record in s3_records:
        all_ok = _process_s3_record(s3_record, deps, message_id=message_id) and all_ok
    return all_ok


def _process_s3_record(s3_record: dict, deps: Dependencies, *, message_id: str) -> bool:
    try:
        event_name = s3_record.get("eventName", "")
        if not event_name.startswith("ObjectCreated:"):
            _log_info(
                "ignoring_non_object_created_event",
                sqs_message_id=message_id,
                event_name=event_name,
            )
            return True

        s3_info = s3_record["s3"]
        bucket = s3_info["bucket"]["name"]
        key = decode_s3_event_key(s3_info["object"]["key"])
        version_id = s3_info["object"].get("versionId")
        if not version_id:
            _log_error(
                "s3_event_missing_version_id", sqs_message_id=message_id, bucket=bucket
            )
            return False
    except (KeyError, TypeError) as exc:
        _log_error("malformed_s3_event_record", sqs_message_id=message_id, error=str(exc))
        return False

    job_id = jobs.compute_job_id(bucket, key, version_id)
    return _process_job(job_id, bucket, key, version_id, deps)


def _process_job(job_id: str, bucket: str, key: str, version_id: str, deps: Dependencies) -> bool:
    claim = deps.job_store.claim(job_id, bucket, key, version_id)

    if claim.status in (jobs.ClaimStatus.ACTIVE_ELSEWHERE, jobs.ClaimStatus.LOST_RACE):
        # Someone else holds (or just won) an active claim. Do NOT drop
        # this message: it is the only trigger that would let the job
        # complete if that other worker crashes, so leave it retryable.
        _log_info("job_claim_deferred", job_id=job_id, claim_status=claim.status.value)
        return False

    if claim.status is jobs.ClaimStatus.ALREADY_TERMINAL:
        assert claim.record is not None
        return _ensure_notified(job_id, claim.record, deps)

    assert claim.status is jobs.ClaimStatus.OWNED
    assert claim.lease_token is not None
    return _run_processing(job_id, bucket, key, version_id, claim.lease_token, deps)


def _run_processing(
    job_id: str, bucket: str, key: str, version_id: str, token: str, deps: Dependencies
) -> bool:
    try:
        content_length = deps.object_storage.head_object(bucket, key, version_id)
    except Exception as exc:  # noqa: BLE001 - classified as a temporary AWS error
        return _fail(job_id, token, deps, error_code="s3_head_object_error", exc=exc)

    if content_length > deps.max_input_bytes:
        return _finish_validation_failed(
            job_id,
            token,
            deps,
            error_code="input_too_large",
            error_message=(
                f"Object is {content_length} bytes, exceeds the "
                f"{deps.max_input_bytes}-byte limit"
            ),
        )

    try:
        handle = deps.object_storage.open_object(bucket, key, version_id)
    except Exception as exc:  # noqa: BLE001
        return _fail(job_id, token, deps, error_code="s3_get_object_error", exc=exc)

    with (
        contextlib.closing(handle.body),
        tempfile.SpooledTemporaryFile(
            max_size=REJECTED_BUFFER_SPOOL_BYTES, mode="w+", encoding="utf-8", newline=""
        ) as rejected_buffer,
    ):
        try:
            result = process_csv(
                handle.body, rejected_buffer, max_bytes=deps.max_input_bytes, schema=deps.schema
            )
        except MalformedCSVError as exc:
            return _finish_validation_failed(
                job_id, token, deps, error_code=exc.code, error_message=exc.message
            )
        except InputTooLargeError as exc:
            return _finish_validation_failed(
                job_id,
                token,
                deps,
                error_code="input_too_large",
                error_message=f"Input exceeded the {exc.max_bytes}-byte limit while streaming",
            )
        except Exception as exc:  # noqa: BLE001 - unexpected processing failure
            return _fail(job_id, token, deps, error_code="processing_error", exc=exc)

        try:
            summary_bytes = summary_json_bytes(result)
            rejected_buffer.seek(0)
            rejected_bytes = rejected_buffer.read().encode("utf-8")
            summary_key, rejected_key = deps.object_storage.put_report(
                deps.output_bucket, job_id, summary_bytes, rejected_bytes
            )
        except Exception as exc:  # noqa: BLE001 - temporary AWS error
            return _fail(
                job_id, token, deps, error_code="s3_put_report_error", exc=exc
            )

    owned = deps.job_store.finalize_success(
        job_id,
        token,
        status=result.status,
        output_summary_key=summary_key,
        output_rejected_key=rejected_key,
        valid_row_count=result.valid_row_count,
        rejected_row_count=result.rejected_row_count,
        error_code=result.error_code,
        error_message=result.error_message,
    )
    if not owned:
        _log_info("lease_lost_before_finalize_deferring_to_other_worker", job_id=job_id)
        return True

    return _notify_and_ack(
        job_id,
        deps,
        lease_owner=token,
        status=result.status,
        valid_row_count=result.valid_row_count,
        rejected_row_count=result.rejected_row_count,
        output_summary_key=summary_key,
        output_rejected_key=rejected_key,
        error_code=result.error_code,
        error_message=result.error_message,
    )


def _finish_validation_failed(
    job_id: str, token: str, deps: Dependencies, *, error_code: str, error_message: str
) -> bool:
    owned = deps.job_store.finalize_validation_failed(
        job_id, token, error_code=error_code, error_message=error_message
    )
    if not owned:
        _log_info("lease_lost_before_finalize_deferring_to_other_worker", job_id=job_id)
        return True

    return _notify_and_ack(
        job_id,
        deps,
        lease_owner=token,
        status=jobs.STATUS_VALIDATION_FAILED,
        valid_row_count=None,
        rejected_row_count=None,
        output_summary_key=None,
        output_rejected_key=None,
        error_code=error_code,
        error_message=error_message,
    )


def _fail(job_id: str, token: str, deps: Dependencies, *, error_code: str, exc: Exception) -> bool:
    deps.job_store.mark_failed(
        job_id, token, error_code=error_code, error_message=_describe_exception(exc)
    )
    _log_error("job_processing_failed", job_id=job_id, error_code=error_code)
    return False


def _ensure_notified(job_id: str, record: dict[str, Any], deps: Dependencies) -> bool:
    if record.get("notification_status") == jobs.NOTIFICATION_SENT:
        _log_info("job_already_completed_and_notified", job_id=job_id)
        return True

    lease_owner = record.get("lease_owner")
    if not lease_owner:
        _log_error("terminal_job_missing_lease_owner", job_id=job_id)
        return True

    _log_info("job_already_completed_retrying_notification", job_id=job_id)
    return _notify_and_ack(
        job_id,
        deps,
        lease_owner=lease_owner,
        status=record["status"],
        valid_row_count=record.get("valid_row_count"),
        rejected_row_count=record.get("rejected_row_count"),
        output_summary_key=record.get("output_summary_key"),
        output_rejected_key=record.get("output_rejected_key"),
        error_code=record.get("error_code"),
        error_message=record.get("error_message"),
    )


def _notify_and_ack(
    job_id: str,
    deps: Dependencies,
    *,
    lease_owner: str,
    status: str,
    valid_row_count: int | None,
    rejected_row_count: int | None,
    output_summary_key: str | None,
    output_rejected_key: str | None,
    error_code: str | None,
    error_message: str | None,
) -> bool:
    subject, body = notifications.build_notification_message(
        job_id=job_id,
        status=status,
        valid_row_count=valid_row_count,
        rejected_row_count=rejected_row_count,
        output_bucket=deps.output_bucket if output_summary_key else None,
        output_summary_key=output_summary_key,
        output_rejected_key=output_rejected_key,
        error_code=error_code,
        error_message=error_message,
    )
    try:
        deps.notifier.publish(job_id=job_id, subject=subject, body=body)
    except Exception as exc:  # noqa: BLE001
        _log_error("sns_publish_failed", job_id=job_id, error=type(exc).__name__)
        deps.job_store.mark_notification_result(job_id, lease_owner, sent=False)
        return False

    deps.job_store.mark_notification_result(job_id, lease_owner, sent=True)
    _log_info("notification_sent", job_id=job_id, status=status)
    return True

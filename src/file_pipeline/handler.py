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
maxReceiveCount is exhausted -- see jobs.py's docstring and
docs/decisions/0002-sqs-between-s3-and-lambda.md for how to recover that).
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
import tempfile
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from file_pipeline import jobs, links, notifications, storage
from file_pipeline.curated import CuratedWriter
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

# Errors that can interrupt reading the object mid-stream (network, S3).
# Any other exception raised while processing comes from this code
# itself and would recur on every attempt with the same bytes.
TRANSIENT_READ_ERRORS = (BotoCoreError, ClientError, OSError)


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


def _emit_attempt_metrics(
    namespace: str, job_id: str, record: dict[str, Any], duration_ms: int
) -> None:
    """One CloudWatch Embedded Metric Format line per finished attempt:
    CloudWatch turns it into metrics (dimension Status) with no API call
    and no extra IAM permission. Counts only, never row content."""
    status = record["status"]
    payload = {
        "level": "INFO",
        "message": "job_attempt_metrics",
        "job_id": job_id,
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [["Status"]],
                    "Metrics": [
                        {"Name": "JobAttempts", "Unit": "Count"},
                        {"Name": "ValidRows", "Unit": "Count"},
                        {"Name": "RejectedRows", "Unit": "Count"},
                        {"Name": "ProcessingMs", "Unit": "Milliseconds"},
                    ],
                }
            ],
        },
        "Status": status,
        "JobAttempts": 1,
        "ValidRows": int(record.get("valid_row_count", 0)),
        "RejectedRows": int(record.get("rejected_row_count", 0)),
        "ProcessingMs": duration_ms,
    }
    logger.info(json.dumps(payload, default=str))


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
    # Must match the queue's redrive maxReceiveCount: the delivery with
    # this receive count is the last one before the dead-letter queue.
    max_receive_count: int = 5
    metrics_namespace: str = "CsvSalesPipeline"
    events: notifications.EventPublisher | None = None
    report_links: links.ReportLinks | None = None


def _default_dependencies() -> Dependencies:
    import boto3

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["JOB_TABLE_NAME"])
    lease_seconds = int(os.environ.get("LEASE_SECONDS", jobs.DEFAULT_LEASE_SECONDS))

    schema = load_schema(os.environ.get("SCHEMA_NAME", DEFAULT_SCHEMA))
    bus_name = os.environ.get("EVENT_BUS_NAME", "")
    events = (
        notifications.EventPublisher(
            boto3.client("events"), bus_name, os.environ.get("EVENT_SOURCE", "csv-sales-pipeline")
        )
        if bus_name
        else None
    )

    portal_url = os.environ.get("PORTAL_URL", "")
    report_links = (
        links.ReportLinks(
            base_url=portal_url,
            key=os.environ["LINK_SIGNING_KEY"].encode(),
            valid_seconds=int(os.environ.get("REPORT_LINK_DAYS", 7)) * 86400,
        )
        if portal_url
        else None
    )

    return Dependencies(
        job_store=jobs.JobStore(
            table, lease_seconds=lease_seconds, rules=(schema.name, schema.version)
        ),
        object_storage=storage.S3Storage(boto3.client("s3")),
        notifier=notifications.SNSNotifier(
            boto3.client("sns"), os.environ["NOTIFICATION_TOPIC_ARN"]
        ),
        output_bucket=os.environ["OUTPUT_BUCKET_NAME"],
        max_input_bytes=int(os.environ.get("MAX_INPUT_BYTES", MAX_INPUT_BYTES)),
        schema=schema,
        max_receive_count=int(os.environ.get("MAX_RECEIVE_COUNT", 5)),
        metrics_namespace=os.environ.get("METRICS_NAMESPACE", "CsvSalesPipeline"),
        events=events,
        report_links=report_links,
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

    receive_count = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
    final_attempt = receive_count >= deps.max_receive_count

    all_ok = True
    for s3_record in s3_records:
        ok = _process_s3_record(
            s3_record, deps, message_id=message_id, final_attempt=final_attempt
        )
        all_ok = ok and all_ok
    return all_ok


def _process_s3_record(
    s3_record: dict, deps: Dependencies, *, message_id: str, final_attempt: bool = False
) -> bool:
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

    if not storage.is_input_key(key):
        _log_info("ignoring_non_csv_object", sqs_message_id=message_id, bucket=bucket)
        return True

    job_id = jobs.compute_job_id(bucket, key, version_id)
    return _process_job(job_id, bucket, key, version_id, deps, final_attempt=final_attempt)


def _process_job(
    job_id: str,
    bucket: str,
    key: str,
    version_id: str,
    deps: Dependencies,
    *,
    final_attempt: bool = False,
) -> bool:
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
    started = time.monotonic()
    ok = _run_processing(
        job_id, bucket, key, version_id, claim.lease_token, deps, final_attempt=final_attempt
    )
    _record_attempt(job_id, claim.lease_token, deps, started)
    return ok


def _record_attempt(job_id: str, token: str, deps: Dependencies, started: float) -> None:
    """Emits metrics for this attempt's outcome, read back from the job
    record so every path (success, failure, duplicate, dead letter) is
    covered in one place. Skipped if another worker owns the job now."""
    try:
        record = deps.job_store.get(job_id)
    except Exception as exc:  # noqa: BLE001 - metrics must never fail a job
        _log_error("attempt_metrics_unavailable", job_id=job_id, error=type(exc).__name__)
        return
    if record is None or record.get("lease_owner") != token:
        return
    if record["status"] == jobs.STATUS_PROCESSING:
        return
    duration_ms = int((time.monotonic() - started) * 1000)
    _emit_attempt_metrics(deps.metrics_namespace, job_id, record, duration_ms)


def _run_processing(
    job_id: str,
    bucket: str,
    key: str,
    version_id: str,
    token: str,
    deps: Dependencies,
    *,
    final_attempt: bool = False,
) -> bool:
    try:
        content_length = deps.object_storage.head_object(bucket, key, version_id)
    except Exception as exc:  # noqa: BLE001 - classified as a temporary AWS error
        return _fail(
            job_id, token, deps, error_code="s3_head_object_error", exc=exc, final=final_attempt
        )

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
        return _fail(
            job_id, token, deps, error_code="s3_get_object_error", exc=exc, final=final_attempt
        )

    # Collects valid rows for Athena while the file streams; they are
    # only uploaded below if the job completes with new content.
    curated = CuratedWriter(deps.schema, job_id) if deps.schema.curated_partition_column else None

    with (
        contextlib.closing(handle.body),
        tempfile.SpooledTemporaryFile(
            max_size=REJECTED_BUFFER_SPOOL_BYTES, mode="w+", encoding="utf-8", newline=""
        ) as rejected_buffer,
        curated or contextlib.nullcontext(),
    ):
        try:
            result = process_csv(
                handle.body,
                rejected_buffer,
                max_bytes=deps.max_input_bytes,
                schema=deps.schema,
                on_valid_row=curated.add if curated else None,
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
        except TRANSIENT_READ_ERRORS as exc:
            return _fail(
                job_id, token, deps, error_code="s3_read_error", exc=exc, final=final_attempt
            )
        except Exception as exc:  # noqa: BLE001 - a bug: retrying the same bytes cannot help
            return _dead_letter(
                job_id,
                token,
                deps,
                error_code="processing_error",
                error_message=_describe_exception(exc),
                in_queue=False,
            )

        # Only a file that produced totals is checked for duplicate
        # content: re-running a failed file cannot double count anything.
        if result.status in jobs.COMPLETED_STATUSES:
            try:
                content = deps.job_store.claim_content(result.content_sha256, job_id)
            except Exception as exc:  # noqa: BLE001 - temporary AWS error
                return _fail(
                    job_id,
                    token,
                    deps,
                    error_code="content_claim_error",
                    exc=exc,
                    final=final_attempt,
                )
            if content.status is jobs.ContentClaimStatus.DUPLICATE:
                return _finish_duplicate(
                    job_id,
                    token,
                    deps,
                    duplicate_of=content.first_job_id,
                    content_sha256=result.content_sha256,
                )
            if content.status is jobs.ContentClaimStatus.PENDING_ELSEWHERE:
                return _wait_for_original(
                    job_id,
                    token,
                    deps,
                    original_job_id=content.first_job_id,
                    final=final_attempt,
                )

        # Reaching here with a completed status means this job owns the
        # content (duplicates returned above), so its rows are new data.
        completed = result.status in jobs.COMPLETED_STATUSES
        try:
            summary_bytes = summary_json_bytes(result)
            rejected_buffer.seek(0)
            rejected_bytes = rejected_buffer.read().encode("utf-8")
            summary_key, rejected_key = deps.object_storage.put_report(
                deps.output_bucket,
                job_id,
                summary_bytes,
                rejected_bytes,
                curated.files() if curated and completed else (),
                manifest={
                    "job_id": job_id,
                    "status": result.status,
                    "error_code": result.error_code,
                    "schema": deps.schema.name,
                    "schema_version": deps.schema.version,
                    "content_sha256": result.content_sha256,
                    "valid_row_count": result.valid_row_count,
                    "rejected_row_count": result.rejected_row_count,
                },
            )
        except Exception as exc:  # noqa: BLE001 - temporary AWS error
            return _fail(
                job_id, token, deps, error_code="s3_put_report_error", exc=exc, final=final_attempt
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
        content_sha256=result.content_sha256,
        warning_counts=_warning_counts(result.warnings),
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
        warning_counts=_warning_counts(result.warnings),
    )


def _int_or_none(value: Any) -> int | None:
    return None if value is None else int(value)


def _warning_counts(warnings: dict[str, Any]) -> dict[str, int]:
    return {name: warning["count"] for name, warning in warnings.items()}


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


def _finish_duplicate(
    job_id: str, token: str, deps: Dependencies, *, duplicate_of: str, content_sha256: str
) -> bool:
    owned = deps.job_store.finalize_duplicate(
        job_id, token, duplicate_of=duplicate_of, content_sha256=content_sha256
    )
    if not owned:
        _log_info("lease_lost_before_finalize_deferring_to_other_worker", job_id=job_id)
        return True

    _log_info("duplicate_content_detected", job_id=job_id, duplicate_of=duplicate_of)
    return _notify_and_ack(
        job_id,
        deps,
        lease_owner=token,
        status=jobs.STATUS_DUPLICATE_CONTENT,
        valid_row_count=None,
        rejected_row_count=None,
        output_summary_key=None,
        output_rejected_key=None,
        error_code=None,
        error_message=None,
        duplicate_of=duplicate_of,
    )


def _wait_for_original(
    job_id: str, token: str, deps: Dependencies, *, original_job_id: str, final: bool = False
) -> bool:
    """Another job is processing the same content and hasn't finished.
    Leave this message retryable: by a later delivery that job has
    completed (this one then ends as a duplicate) or failed and is
    itself being retried. Logged at INFO: waiting is not an error."""
    error_message = f"Same content as job {original_job_id}, which has not finished yet"
    if final:
        return _dead_letter(
            job_id,
            token,
            deps,
            error_code="waiting_for_original_job",
            error_message=error_message,
            in_queue=True,
        )
    deps.job_store.mark_failed(
        job_id, token, error_code="waiting_for_original_job", error_message=error_message
    )
    _log_info("duplicate_waiting_for_original_job", job_id=job_id, original_job_id=original_job_id)
    return False


def _fail(
    job_id: str,
    token: str,
    deps: Dependencies,
    *,
    error_code: str,
    exc: Exception,
    final: bool = False,
) -> bool:
    """A failure worth retrying. On the last allowed delivery the job is
    dead-lettered instead, so it does not sit at "failed" forever."""
    if final:
        return _dead_letter(
            job_id,
            token,
            deps,
            error_code=error_code,
            error_message=_describe_exception(exc),
            in_queue=True,
        )
    deps.job_store.mark_failed(
        job_id, token, error_code=error_code, error_message=_describe_exception(exc)
    )
    _log_error("job_processing_failed", job_id=job_id, error_code=error_code)
    return False


def _dead_letter(
    job_id: str,
    token: str,
    deps: Dependencies,
    *,
    error_code: str,
    error_message: str,
    in_queue: bool,
) -> bool:
    """Stops retrying a job and says how to resume it.

    in_queue=True: the last allowed delivery failed; returning False lets
    SQS move the message to the dead-letter queue, from which a redrive
    resumes the job. in_queue=False: the error would recur on every
    attempt, so the message is acknowledged now and the job is resumed
    with scripts/reprocess.py once the cause is fixed."""
    if not deps.job_store.mark_dead_lettered(
        job_id, token, error_code=error_code, error_message=error_message
    ):
        _log_info("lease_lost_before_finalize_deferring_to_other_worker", job_id=job_id)
        return True
    _log_error("job_dead_lettered", job_id=job_id, error_code=error_code, in_queue=in_queue)

    if in_queue:
        next_step = (
            "The message is in the dead-letter queue. Fix the cause, then move it "
            "back with scripts/redrive_dlq.sh."
        )
    else:
        next_step = (
            "This error would repeat on every attempt, so retrying stopped. Fix the "
            f"cause, then run: scripts/reprocess.py --job-id {job_id}"
        )
    subject, body = notifications.build_notification_message(
        job_id=job_id,
        status=jobs.STATUS_DEAD_LETTERED,
        valid_row_count=None,
        rejected_row_count=None,
        output_bucket=None,
        output_summary_key=None,
        output_rejected_key=None,
        error_code=error_code,
        error_message=error_message,
        next_step=next_step,
    )
    try:
        deps.notifier.publish(job_id=job_id, subject=subject, body=body)
    except Exception as exc:  # noqa: BLE001 - best effort; the job record and alarm remain
        _log_error("sns_publish_failed", job_id=job_id, error=type(exc).__name__)
    try:
        _publish_event(
            deps,
            job_id,
            detail_type="CSV job dead-lettered",
            detail={
                "status": jobs.STATUS_DEAD_LETTERED,
                "error_code": error_code,
                "in_dead_letter_queue": in_queue,
            },
        )
    except Exception as exc:  # noqa: BLE001 - best effort, as above
        _log_error("event_publish_failed", job_id=job_id, error=type(exc).__name__)
    return not in_queue


def _publish_event(
    deps: Dependencies, job_id: str, *, detail_type: str, detail: dict[str, Any]
) -> None:
    """Publishes to EventBridge, if enabled, adding the job's source file
    (read from the job record) so subscribers need no lookup."""
    if deps.events is None:
        return
    record = deps.job_store.get(job_id) or {}
    deps.events.publish(
        detail_type=detail_type,
        detail={
            "job_id": job_id,
            **detail,
            "source_bucket": record.get("source_bucket"),
            "source_key": record.get("source_key"),
            "source_version_id": record.get("source_version_id"),
        },
    )


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
        duplicate_of=record.get("duplicate_of"),
        warning_counts=record.get("warning_counts"),
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
    duplicate_of: str | None = None,
    warning_counts: dict[str, int] | None = None,
) -> bool:
    download_links = links_expire_at = None
    if deps.report_links is not None and output_summary_key:
        download_links, expires_at = deps.report_links.for_job(job_id, now=time.time())
        links_expire_at = datetime.fromtimestamp(expires_at, UTC).strftime("%Y-%m-%d %H:%M UTC")
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
        duplicate_of=duplicate_of,
        warning_counts=warning_counts,
        download_links=download_links,
        links_expire_at=links_expire_at,
    )
    try:
        deps.notifier.publish(job_id=job_id, subject=subject, body=body)
    except Exception as exc:  # noqa: BLE001
        _log_error("sns_publish_failed", job_id=job_id, error=type(exc).__name__)
        deps.job_store.mark_notification_result(job_id, lease_owner, sent=False)
        return False

    try:
        _publish_event(
            deps,
            job_id,
            detail_type="CSV job finished",
            detail={
                "status": status,
                "valid_row_count": _int_or_none(valid_row_count),
                "rejected_row_count": _int_or_none(rejected_row_count),
                "error_code": error_code,
                "duplicate_of": duplicate_of,
                "warning_counts": {k: int(v) for k, v in (warning_counts or {}).items()},
                "output_bucket": deps.output_bucket if output_summary_key else None,
                "summary_key": output_summary_key,
                "rejected_key": output_rejected_key,
            },
        )
    except Exception as exc:  # noqa: BLE001 - retried with the notification
        _log_error("event_publish_failed", job_id=job_id, error=type(exc).__name__)
        deps.job_store.mark_notification_result(job_id, lease_owner, sent=False)
        return False

    deps.job_store.mark_notification_result(job_id, lease_owner, sent=True)
    _log_info("notification_sent", job_id=job_id, status=status)
    return True

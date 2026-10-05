"""DynamoDB job state, atomic claims, and lease-based crash recovery.

Job identity
------------
A job's identity is derived, never generated: `job_id = sha256(bucket \\n
key \\n version_id)`. Re-delivering the same S3 event notification (SQS's
at-least-once guarantee) always maps to the same job_id, which is what
makes the claim logic below able to detect duplicates.

Claim state machine
--------------------
Each job item has a `status` and a `lease_owner` (an opaque token unique
to one processing attempt) with a `lease_expires_at` (epoch seconds).
`claim()` implements:

  * no item exists            -> conditional PutItem creates it (owned)
  * status is terminal        -> ALREADY_TERMINAL (never reprocess)
  * status=processing, lease
    not yet expired           -> ACTIVE_ELSEWHERE (leave message retryable)
  * status=processing, lease
    expired                   -> conditional steal keyed on the expiry
                                  check staying true (owned, or LOST_RACE
                                  if another worker stole it first)
  * status=failed or
    dead_lettered              -> conditional reclaim keyed on status
                                  staying the same (owned, or LOST_RACE)

`failed` means an attempt did not finish and SQS will deliver the
message again. `dead_lettered` means retrying stopped: either the last
allowed delivery failed (the message is now in the dead-letter queue and
a redrive resumes it), or processing hit an error that would recur on
every attempt (the message was acknowledged; scripts/reprocess.py
resumes it after a fix). Neither is final, so both can be reclaimed.

Every state-changing write after the initial claim is itself conditioned
on `lease_owner == <our token>`, so a worker whose lease has since been
stolen by someone else can never clobber that newer worker's result --
its writes simply fail their condition and it backs off.

Duplicate content
-----------------
Job identity is per S3 object version, so the same bytes uploaded under
a new key (or again under the same key) are a new job. Before a job
that produced totals writes its reports, `claim_content()` records the
file's SHA-256 in a fingerprint item (`job_id = "content#<sha256>"`, in
the same table; real job IDs are bare hex and can never collide):

  * no fingerprint yet, or it names this job -> FIRST (write reports)
  * names another job that completed         -> DUPLICATE (that job's
                                                 reports already cover
                                                 this content)
  * names another job not yet finished       -> PENDING_ELSEWHERE (retry
                                                 later; never drop it)

Files that fail validation never claim a fingerprint: they produce no
totals, so re-running one cannot double count anything.
"""

from __future__ import annotations

import hashlib
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from botocore.exceptions import ClientError

STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_COMPLETED_WITH_REJECTIONS = "completed_with_rejections"
STATUS_VALIDATION_FAILED = "validation_failed"
STATUS_DUPLICATE_CONTENT = "duplicate_content"
STATUS_FAILED = "failed"
STATUS_DEAD_LETTERED = "dead_lettered"

RECLAIMABLE_STATUSES = frozenset({STATUS_FAILED, STATUS_DEAD_LETTERED})

TERMINAL_STATUSES = frozenset(
    {
        STATUS_COMPLETED,
        STATUS_COMPLETED_WITH_REJECTIONS,
        STATUS_VALIDATION_FAILED,
        STATUS_DUPLICATE_CONTENT,
    }
)
COMPLETED_STATUSES = frozenset({STATUS_COMPLETED, STATUS_COMPLETED_WITH_REJECTIONS})

NOTIFICATION_PENDING = "pending"
NOTIFICATION_SENT = "sent"
NOTIFICATION_FAILED = "failed"

DEFAULT_LEASE_SECONDS = 300

_CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailedException"
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def compute_job_id(bucket: str, key: str, version_id: str) -> str:
    digest_input = f"{bucket}\n{key}\n{version_id}".encode()
    return hashlib.sha256(digest_input).hexdigest()


def content_fingerprint_key(content_sha256: str) -> str:
    return f"content#{content_sha256}"


def sanitize_error_message(message: str, max_len: int = 500) -> str:
    """Strips control characters and truncates, so a stack trace or
    unexpected exception text can never corrupt a DynamoDB item, a log
    line, or an outbound SNS message."""
    cleaned = _CONTROL_CHARS_RE.sub(" ", message).strip()
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 3] + "..."
    return cleaned


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class ClaimStatus(StrEnum):
    OWNED = "owned"
    ALREADY_TERMINAL = "already_terminal"
    ACTIVE_ELSEWHERE = "active_elsewhere"
    LOST_RACE = "lost_race"


@dataclass
class ClaimResult:
    status: ClaimStatus
    lease_token: str | None = None
    record: dict[str, Any] | None = None


class ContentClaimStatus(StrEnum):
    FIRST = "first"
    DUPLICATE = "duplicate"
    PENDING_ELSEWHERE = "pending_elsewhere"


@dataclass
class ContentClaimResult:
    status: ContentClaimStatus
    first_job_id: str


def _is_conditional_check_failed(exc: ClientError) -> bool:
    return exc.response.get("Error", {}).get("Code") == _CONDITIONAL_CHECK_FAILED


class JobStore:
    def __init__(self, table: Any, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> None:
        self._table = table
        self._lease_seconds = lease_seconds

    def get(self, job_id: str) -> dict[str, Any] | None:
        response = self._table.get_item(Key={"job_id": job_id}, ConsistentRead=True)
        return response.get("Item")

    def claim(
        self, job_id: str, source_bucket: str, source_key: str, source_version_id: str
    ) -> ClaimResult:
        from boto3.dynamodb.conditions import Attr

        now = int(time.time())
        token = uuid.uuid4().hex
        lease_expires_at = now + self._lease_seconds

        try:
            self._table.put_item(
                Item={
                    "job_id": job_id,
                    "source_bucket": source_bucket,
                    "source_key": source_key,
                    "source_version_id": source_version_id,
                    "status": STATUS_PROCESSING,
                    "lease_owner": token,
                    "lease_expires_at": lease_expires_at,
                    "attempt_count": 1,
                    "created_at": _now_iso(),
                    "updated_at": _now_iso(),
                    "notification_status": NOTIFICATION_PENDING,
                },
                ConditionExpression=Attr("job_id").not_exists(),
            )
            return ClaimResult(status=ClaimStatus.OWNED, lease_token=token)
        except ClientError as exc:
            if not _is_conditional_check_failed(exc):
                raise

        record = self.get(job_id)
        if record is None:
            # The competing writer's item vanished from a consistent read
            # right after our put lost the race -- we never delete job
            # items, so this should not happen. Leave it retryable.
            return ClaimResult(status=ClaimStatus.LOST_RACE)

        status = record["status"]
        if status in TERMINAL_STATUSES:
            return ClaimResult(status=ClaimStatus.ALREADY_TERMINAL, record=record)

        if status == STATUS_PROCESSING:
            if int(record["lease_expires_at"]) >= now:
                return ClaimResult(status=ClaimStatus.ACTIVE_ELSEWHERE)
            condition = Attr("status").eq(STATUS_PROCESSING) & Attr("lease_expires_at").lt(now)
        elif status in RECLAIMABLE_STATUSES:
            condition = Attr("status").eq(status)
        else:
            return ClaimResult(status=ClaimStatus.LOST_RACE)

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=(
                    "SET #status = :processing, lease_owner = :token, "
                    "lease_expires_at = :lease_expires_at, "
                    "attempt_count = attempt_count + :one, "
                    "updated_at = :updated_at, "
                    "notification_status = :notif_pending"
                ),
                ConditionExpression=condition,
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":processing": STATUS_PROCESSING,
                    ":token": token,
                    ":lease_expires_at": lease_expires_at,
                    ":one": 1,
                    ":updated_at": _now_iso(),
                    ":notif_pending": NOTIFICATION_PENDING,
                },
            )
            return ClaimResult(status=ClaimStatus.OWNED, lease_token=token)
        except ClientError as exc:
            if _is_conditional_check_failed(exc):
                return ClaimResult(status=ClaimStatus.LOST_RACE)
            raise

    def claim_content(self, content_sha256: str, job_id: str) -> ContentClaimResult:
        """Records `job_id` as the first job to process this exact
        content, unless another job already did. See the module
        docstring for the three outcomes."""
        from boto3.dynamodb.conditions import Attr

        fingerprint_key = content_fingerprint_key(content_sha256)
        try:
            self._table.put_item(
                Item={
                    "job_id": fingerprint_key,
                    "record_type": "content_fingerprint",
                    "first_job_id": job_id,
                    "created_at": _now_iso(),
                },
                ConditionExpression=Attr("job_id").not_exists(),
            )
            return ContentClaimResult(ContentClaimStatus.FIRST, job_id)
        except ClientError as exc:
            if not _is_conditional_check_failed(exc):
                raise

        fingerprint = self.get(fingerprint_key)
        first_job_id = fingerprint["first_job_id"] if fingerprint else ""
        if first_job_id == job_id:
            # A retry of the job that claimed this content first.
            return ContentClaimResult(ContentClaimStatus.FIRST, job_id)

        first_job = self.get(first_job_id) if first_job_id else None
        if first_job is not None and first_job["status"] in COMPLETED_STATUSES:
            return ContentClaimResult(ContentClaimStatus.DUPLICATE, first_job_id)
        return ContentClaimResult(ContentClaimStatus.PENDING_ELSEWHERE, first_job_id)

    def finalize_duplicate(
        self, job_id: str, token: str, *, duplicate_of: str, content_sha256: str
    ) -> bool:
        from boto3.dynamodb.conditions import Attr

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=(
                    "SET #status = :status, updated_at = :updated_at, "
                    "duplicate_of = :duplicate_of, content_sha256 = :content_sha256, "
                    "notification_status = :notif_pending "
                    "REMOVE error_code, error_message"
                ),
                ConditionExpression=Attr("lease_owner").eq(token),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":status": STATUS_DUPLICATE_CONTENT,
                    ":updated_at": _now_iso(),
                    ":duplicate_of": duplicate_of,
                    ":content_sha256": content_sha256,
                    ":notif_pending": NOTIFICATION_PENDING,
                },
            )
            return True
        except ClientError as exc:
            if _is_conditional_check_failed(exc):
                return False
            raise

    def finalize_success(
        self,
        job_id: str,
        token: str,
        *,
        status: str,
        output_summary_key: str,
        output_rejected_key: str,
        valid_row_count: int,
        rejected_row_count: int,
        error_code: str | None = None,
        error_message: str | None = None,
        content_sha256: str | None = None,
        warning_counts: dict[str, int] | None = None,
    ) -> bool:
        """Records a run that produced reports. `error_code` is set when
        the file still failed validation (no valid rows, or too many
        rejected ones); otherwise any error from an earlier failed
        attempt is cleared."""
        from boto3.dynamodb.conditions import Attr

        values = {
            ":status": status,
            ":updated_at": _now_iso(),
            ":summary_key": output_summary_key,
            ":rejected_key": output_rejected_key,
            ":valid_count": valid_row_count,
            ":rejected_count": rejected_row_count,
            ":notif_pending": NOTIFICATION_PENDING,
        }
        update_expression = (
            "SET #status = :status, updated_at = :updated_at, "
            "output_summary_key = :summary_key, "
            "output_rejected_key = :rejected_key, "
            "valid_row_count = :valid_count, "
            "rejected_row_count = :rejected_count, "
            "notification_status = :notif_pending"
        )
        if content_sha256 is not None:
            update_expression += ", content_sha256 = :content_sha256"
            values[":content_sha256"] = content_sha256
        if warning_counts:
            update_expression += ", warning_counts = :warning_counts"
            values[":warning_counts"] = warning_counts
        if error_code is None:
            update_expression += " REMOVE error_code, error_message"
        else:
            update_expression += ", error_code = :error_code, error_message = :error_message"
            values[":error_code"] = error_code
            values[":error_message"] = sanitize_error_message(error_message or "")

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=update_expression,
                ConditionExpression=Attr("lease_owner").eq(token),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues=values,
            )
            return True
        except ClientError as exc:
            if _is_conditional_check_failed(exc):
                return False
            raise

    def finalize_validation_failed(
        self, job_id: str, token: str, *, error_code: str, error_message: str
    ) -> bool:
        from boto3.dynamodb.conditions import Attr

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=(
                    "SET #status = :status, updated_at = :updated_at, "
                    "error_code = :error_code, error_message = :error_message, "
                    "notification_status = :notif_pending"
                ),
                ConditionExpression=Attr("lease_owner").eq(token),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":status": STATUS_VALIDATION_FAILED,
                    ":updated_at": _now_iso(),
                    ":error_code": error_code,
                    ":error_message": sanitize_error_message(error_message),
                    ":notif_pending": NOTIFICATION_PENDING,
                },
            )
            return True
        except ClientError as exc:
            if _is_conditional_check_failed(exc):
                return False
            raise

    def mark_failed(self, job_id: str, token: str, *, error_code: str, error_message: str) -> bool:
        """This attempt did not finish; SQS will deliver the message again."""
        return self._mark_unfinished(
            job_id, token, STATUS_FAILED, error_code=error_code, error_message=error_message
        )

    def mark_dead_lettered(
        self, job_id: str, token: str, *, error_code: str, error_message: str
    ) -> bool:
        """Retrying has stopped; see the module docstring."""
        return self._mark_unfinished(
            job_id, token, STATUS_DEAD_LETTERED, error_code=error_code, error_message=error_message
        )

    def _mark_unfinished(
        self, job_id: str, token: str, status: str, *, error_code: str, error_message: str
    ) -> bool:
        from boto3.dynamodb.conditions import Attr

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=(
                    "SET #status = :status, updated_at = :updated_at, "
                    "error_code = :error_code, error_message = :error_message"
                ),
                ConditionExpression=Attr("lease_owner").eq(token),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":status": status,
                    ":updated_at": _now_iso(),
                    ":error_code": error_code,
                    ":error_message": sanitize_error_message(error_message),
                },
            )
            return True
        except ClientError as exc:
            if _is_conditional_check_failed(exc):
                return False
            raise

    def mark_notification_result(self, job_id: str, lease_owner: str, *, sent: bool) -> bool:
        """Records the outcome of an SNS publish attempt for a job that
        has already reached a terminal processing status. Conditioned on
        the lease_owner the caller read (either the token it just
        finalized with, or the lease_owner already on an already-terminal
        record it is retrying notification for)."""
        from boto3.dynamodb.conditions import Attr

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=(
                    "SET notification_status = :notif_status, updated_at = :updated_at, "
                    "notification_attempt_count = "
                    "if_not_exists(notification_attempt_count, :zero) + :one"
                ),
                ConditionExpression=Attr("lease_owner").eq(lease_owner),
                ExpressionAttributeValues={
                    ":notif_status": NOTIFICATION_SENT if sent else NOTIFICATION_FAILED,
                    ":updated_at": _now_iso(),
                    ":zero": 0,
                    ":one": 1,
                },
            )
            return True
        except ClientError as exc:
            if _is_conditional_check_failed(exc):
                return False
            raise

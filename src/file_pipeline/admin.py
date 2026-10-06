"""Operator tasks run from a laptop, not by the Lambda: finding jobs and
deliberately reprocessing them (see scripts/reprocess.py).

Reprocessing re-runs a job exactly as a retry would. The job is marked
`failed` with error_code `reprocess_requested` -- a status the claim
logic reclaims (see jobs.py) -- and its original S3 event is put back on
the processing queue. The Lambda then reads the same object version
again, under the rules it currently runs with, and overwrites the job's
reports, curated files, and manifest at their deterministic keys, so
nothing is counted twice.
"""

from __future__ import annotations

import json
import urllib.parse
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import ClientError

from file_pipeline import jobs

STATUS_INDEX = "status-created_at-index"

# Everything except "processing": a job whose lease is live is left alone.
REPROCESSABLE_STATUSES = frozenset(
    {*jobs.TERMINAL_STATUSES, jobs.STATUS_FAILED, jobs.STATUS_DEAD_LETTERED}
)


def s3_event_message(bucket: str, key: str, version_id: str) -> str:
    """An SQS message body shaped like S3's own event notification.
    Keys are form-encoded the way S3 encodes them, which is what
    handler.decode_s3_event_key reverses."""
    return json.dumps(
        {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "eventSource": "csv-sales-pipeline:reprocess",
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {
                            "key": urllib.parse.quote_plus(key, safe=""),
                            "versionId": version_id,
                        },
                    },
                }
            ]
        }
    )


def jobs_with_status(table: Any, status: str) -> Iterator[dict[str, Any]]:
    """Newest first, via the status index (content fingerprint items have
    no status, so they are never returned)."""
    from boto3.dynamodb.conditions import Key

    kwargs: dict[str, Any] = {
        "IndexName": STATUS_INDEX,
        "KeyConditionExpression": Key("status").eq(status),
        "ScanIndexForward": False,
    }
    while True:
        page = table.query(**kwargs)
        yield from page.get("Items", [])
        if "LastEvaluatedKey" not in page:
            return
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def processed_before_version(job: dict[str, Any], version: int) -> bool:
    """True when the job's latest attempt ran under an older schema
    version (or before versions were recorded)."""
    return int(job.get("schema_version", 0)) < version


def request_reprocess(table: Any, sqs: Any, queue_url: str, job: dict[str, Any]) -> bool:
    """Marks one job retryable and queues its original S3 event. Returns
    False, changing nothing, if the job is running or changed status
    since it was read."""
    from boto3.dynamodb.conditions import Attr

    if job["status"] not in REPROCESSABLE_STATUSES:
        return False
    try:
        table.update_item(
            Key={"job_id": job["job_id"]},
            UpdateExpression=(
                "SET #status = :failed, error_code = :code, error_message = :message, "
                "updated_at = :now"
            ),
            ConditionExpression=Attr("status").eq(job["status"]),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":failed": jobs.STATUS_FAILED,
                ":code": "reprocess_requested",
                ":message": f"Reprocess requested (was {job['status']})",
                ":now": datetime.now(UTC).isoformat(),
            },
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=s3_event_message(
            job["source_bucket"], job["source_key"], job["source_version_id"]
        ),
    )
    return True

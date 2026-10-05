"""SNS completion notifications.

Notification state is tracked separately from processing state (see
jobs.py's `notification_status` field) precisely so that an SNS failure
after a successful processing run only needs to retry the publish, never
the CSV processing itself.

Note on exactly-once delivery: publishing to SNS and then recording that
success in DynamoDB are two separate operations. If the process crashes
or is throttled between them, the next attempt will see
notification_status != "sent" and publish again, so the subscriber may
occasionally receive a duplicate notification for the same job. This is
accepted, documented behavior -- treat notifications as at-least-once,
not exactly-once. It never causes duplicate CSV processing because
notification retries are gated on the job already being terminal.
"""

from __future__ import annotations

import json
from typing import Any

_WARNING_LABELS = {"duplicate_rows": "repeated row", "outliers": "unusual value"}


def build_notification_message(
    *,
    job_id: str,
    status: str,
    valid_row_count: int | None,
    rejected_row_count: int | None,
    output_bucket: str | None,
    output_summary_key: str | None,
    output_rejected_key: str | None,
    error_code: str | None,
    error_message: str | None,
    duplicate_of: str | None = None,
    warning_counts: dict[str, int] | None = None,
    next_step: str | None = None,
) -> tuple[str, str]:
    """Returns (subject, body). Reports are private S3 objects (no public
    access) -- the notification names their bucket/key so an authorized
    recipient can fetch them via the console or `aws s3 cp`, never a
    public URL."""
    subject = f"CSV job {status}: {job_id[:12]}"

    lines = [f"Job ID: {job_id}", f"Status: {status}"]

    if valid_row_count is not None and rejected_row_count is not None:
        lines.append(f"Valid rows: {valid_row_count}")
        lines.append(f"Rejected rows: {rejected_row_count}")

    flagged = {name: int(count) for name, count in (warning_counts or {}).items() if count}
    if flagged:
        parts = [
            f"{count} {_WARNING_LABELS.get(name, name)}{'' if count == 1 else 's'}"
            for name, count in flagged.items()
        ]
        lines.append(f"Warnings: {', '.join(parts)} (row numbers are in summary.json)")

    if output_summary_key and output_rejected_key and output_bucket:
        lines.append(f"Summary report: s3://{output_bucket}/{output_summary_key}")
        lines.append(f"Rejected rows report: s3://{output_bucket}/{output_rejected_key}")

    if duplicate_of:
        lines.append(f"Duplicate of job: {duplicate_of}")
        lines.append(
            "This file's content was already processed by that job, "
            "so no new report was written."
        )

    if error_code:
        lines.append(f"Error code: {error_code}")
    if error_message:
        lines.append(f"Error: {error_message}")
    if next_step:
        lines.append(f"Next step: {next_step}")

    return subject, "\n".join(lines)


class SNSNotifier:
    def __init__(self, sns_client: Any, topic_arn: str) -> None:
        self._sns = sns_client
        self._topic_arn = topic_arn

    def publish(self, *, job_id: str, subject: str, body: str) -> None:
        # SNS subjects are capped at 100 characters.
        self._sns.publish(
            TopicArn=self._topic_arn,
            Subject=subject[:100],
            Message=body,
            MessageAttributes={
                "job_id": {"DataType": "String", "StringValue": job_id},
            },
        )


class EventPublisher:
    """Publishes job outcomes to an EventBridge bus, so other systems can
    subscribe to exactly the outcomes they care about (for example only
    validation_failed) with an EventBridge rule. Delivery is at least once,
    like the SNS notification it accompanies."""

    def __init__(self, events_client: Any, bus_name: str, source: str) -> None:
        self._events = events_client
        self._bus_name = bus_name
        self._source = source

    def publish(self, *, detail_type: str, detail: dict[str, Any]) -> None:
        response = self._events.put_events(
            Entries=[
                {
                    "EventBusName": self._bus_name,
                    "Source": self._source,
                    "DetailType": detail_type,
                    "Detail": json.dumps(detail),
                }
            ]
        )
        if response.get("FailedEntryCount"):
            code = response["Entries"][0].get("ErrorCode", "Unknown")
            raise RuntimeError(f"EventBridge rejected the event: {code}")

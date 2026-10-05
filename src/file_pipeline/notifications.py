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

from typing import Any


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

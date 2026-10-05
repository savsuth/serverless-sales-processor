# 0013. Jobs whose retries stop get a dead_lettered status

Status: accepted (2026-10-05)

## Context

A job whose message ran out of retries stayed `failed` forever, with
only a dead-letter-queue alarm to show for it. An exception from the
processing code itself was retried five times on the same bytes,
failing identically each time.

## Decision

- On the last allowed delivery (SQS `ApproximateReceiveCount` reaching
  `MAX_RECEIVE_COUNT`), a retryable failure marks the job
  `dead_lettered` and notifies, then lets SQS move the message to the
  dead-letter queue; `scripts/redrive_dlq.sh` resumes it.
- An exception raised by the processing code would recur on every
  attempt, so the job is `dead_lettered` immediately and the message
  acknowledged; the notification names `scripts/reprocess.py`. Errors
  while reading the object (`BotoCoreError`, `ClientError`, `OSError`)
  are classed as `s3_read_error` and retried normally.
- `dead_lettered` is not final: like `failed`, the claim logic can
  reclaim it.

## Consequences

- Every stuck job is visible by status (`scripts/list_jobs.sh <table>
  dead_lettered`) and announced, by SNS and EventBridge.
- A parked bug does not reach the dead-letter queue, so the DLQ alarm
  does not fire for it; the ERROR log line does fire the handler-error
  alarm.

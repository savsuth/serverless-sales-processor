# 0002. An SQS queue sits between S3 and Lambda

Status: accepted (original build; recorded 2026-10-04)

## Context

S3 can invoke a Lambda function directly, but the pipeline needs a
durable buffer when uploads arrive faster than they are processed, a
fixed and visible retry count, a place where messages that keep failing
are kept for inspection, and a cap on how many files are processed at
once.

## Decision

S3 sends object-created events to a standard SQS queue (S3
notifications support only standard queues, not FIFO). The Lambda
consumes it through an event source mapping with
`ReportBatchItemFailures`, so only the failed records in a batch are
retried. After `sqs_max_receive_count` (5) failed receives a message
moves to a dead-letter queue, and `scripts/redrive_dlq.sh` moves it back
once the cause is fixed.

Concurrency is capped with the event source mapping's
`maximum_concurrency` (`lambda_max_concurrency`, default 5), not with
reserved concurrency on the function.

## Consequences

- Reserved concurrency was rejected because a throttled SQS-triggered
  invocation returns its messages to the queue and each return counts
  toward the receive limit, so healthy messages can reach the
  dead-letter queue during a burst. It also fails outright on new
  accounts whose total concurrency is at the 10-execution minimum.
- `maximum_concurrency` must be between 2 and 1000, enforced by a
  Terraform variable validation.
- Batch size stays at 1: one heavy file cannot use up the time budget
  of others in the same invocation.

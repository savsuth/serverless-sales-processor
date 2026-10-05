# 0015. Outcomes are measured with embedded metrics and announced on EventBridge

Status: accepted (2026-10-05)

## Context

Alarms covered errors and the dead-letter queue, but nothing showed
throughput or outcomes over time, nothing caught a queue nobody was
consuming, and other systems could learn about outcomes only by parsing
email.

## Decision

- After each attempt the Lambda logs one CloudWatch Embedded Metric
  Format line (`JobAttempts`, `ValidRows`, `RejectedRows`,
  `ProcessingMs`, by `Status`), read back from the job record so every
  path is covered in one place. CloudWatch extracts the metrics from the
  log, with no API call or extra permission.
- Metrics use the namespace `CsvSalesPipeline/<project_name>`, so stacks
  never mix.
- `infra/dashboard.tf` shows outcomes, rows, Lambda health, queues and
  Athena usage.
- A queue-stuck alarm fires when the oldest message has waited longer
  than its whole retry budget (visibility timeout x max receives).
- Each outcome is also published to the default EventBridge bus
  ("CSV job finished", "CSV job dead-lettered"; source = project name),
  with the source file, counts and report keys. It is retried with the
  SNS notification, so delivery is at least once.

## Consequences

- Attempt metrics count retries: a job that failed once then completed
  shows one `failed` and one `completed` attempt.
- One extra DynamoDB read per attempt (metrics) and per published event
  (source file).

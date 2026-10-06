# One dashboard per stack: job outcomes and rows (from the Lambda's
# embedded-format metrics), Lambda health, queues, and Athena usage.

locals {
  job_statuses = [
    "completed",
    "completed_with_rejections",
    "validation_failed",
    "duplicate_content",
    "failed",
    "dead_lettered",
  ]

  function_name = aws_lambda_function.processor.function_name

  dashboard_widgets = concat(
    [
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 2
        properties = {
          markdown = join("", [
            "## ${var.project_name}\n",
            "Job outcomes are counted per attempt (a retried job shows a `failed` attempt, then its final outcome). ",
            "Alarms publish to the SNS topic; runbooks are in `docs/decisions/` and `scripts/`.",
          ])
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 2
        width  = 12
        height = 6
        properties = {
          title   = "Job attempts by outcome"
          region  = var.aws_region
          view    = "timeSeries"
          stacked = true
          stat    = "Sum"
          period  = 300
          metrics = [for s in local.job_statuses : [local.metrics_namespace, "JobAttempts", "Status", s]]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 2
        width  = 12
        height = 6
        properties = {
          title  = "Rows processed"
          region = var.aws_region
          view   = "timeSeries"
          period = 300
          metrics = [
            [{ id = "valid", label = "Valid rows", expression = "SUM(SEARCH('{${local.metrics_namespace},Status} MetricName=\"ValidRows\"', 'Sum', 300))" }],
            [{ id = "rejected", label = "Rejected rows", expression = "SUM(SEARCH('{${local.metrics_namespace},Status} MetricName=\"RejectedRows\"', 'Sum', 300))" }],
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 8
        width  = 12
        height = 6
        properties = {
          title  = "Processor Lambda"
          region = var.aws_region
          view   = "timeSeries"
          period = 300
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", local.function_name, { stat = "p95", label = "Duration p95 (ms)" }],
            ["AWS/Lambda", "Duration", "FunctionName", local.function_name, { stat = "Maximum", label = "Duration max (ms)" }],
            ["AWS/Lambda", "Errors", "FunctionName", local.function_name, { stat = "Sum", yAxis = "right" }],
            ["AWS/Lambda", "Throttles", "FunctionName", local.function_name, { stat = "Sum", yAxis = "right" }],
            [local.metrics_namespace, "HandlerErrorLogs", { stat = "Sum", yAxis = "right", label = "ERROR log lines" }],
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 8
        width  = 12
        height = 6
        properties = {
          title  = "Queues"
          region = var.aws_region
          view   = "timeSeries"
          period = 300
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.processing.name, { stat = "Maximum", label = "Waiting" }],
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.dlq.name, { stat = "Maximum", label = "Dead-letter queue" }],
            ["AWS/SQS", "ApproximateAgeOfOldestMessage", "QueueName", aws_sqs_queue.processing.name, { stat = "Maximum", label = "Oldest message (s)", yAxis = "right" }],
          ]
          annotations = {
            horizontal = [{ label = "Stuck-queue alarm", value = local.queue_stuck_seconds, yAxis = "right" }]
          }
        }
      },
    ],
    local.athena_enabled ? [
      {
        type   = "metric"
        x      = 0
        y      = 14
        width  = 12
        height = 6
        properties = {
          title  = "Athena data scanned"
          region = var.aws_region
          view   = "timeSeries"
          period = 3600
          metrics = [
            [{ id = "scanned", label = "Bytes scanned", expression = "SUM(SEARCH('{AWS/Athena,QueryState,QueryType,WorkGroup} MetricName=\"ProcessedBytes\" WorkGroup=\"${aws_athena_workgroup.analytics[0].name}\"', 'Sum', 3600))" }],
          ]
        }
      },
    ] : [],
  )
}

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = var.project_name
  dashboard_body = jsonencode({ widgets = local.dashboard_widgets })
}

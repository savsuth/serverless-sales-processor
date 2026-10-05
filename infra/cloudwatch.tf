resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project_name}-processor"
  retention_in_days = var.log_retention_days
}

# Alarms publish to the same notification topic as job-completion
# messages. That keeps this template to one topic/one optional email
# subscription; split them into a dedicated "ops" topic if job-completion
# volume would otherwise bury alarm emails.

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name        = "${var.project_name}-lambda-errors"
  alarm_description = "The processor Lambda raised one or more unhandled errors."
  namespace         = "AWS/Lambda"
  metric_name       = "Errors"
  dimensions = {
    FunctionName = aws_lambda_function.processor.function_name
  }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.notifications.arn]
  ok_actions          = [aws_sns_topic.notifications.arn]
}

resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  alarm_name        = "${var.project_name}-dlq-messages"
  alarm_description = "One or more messages have landed in the dead-letter queue and need investigation. Fix the cause, then move them back with scripts/redrive_dlq.sh."
  namespace         = "AWS/SQS"
  metric_name       = "ApproximateNumberOfMessagesVisible"
  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.notifications.arn]
  ok_actions          = [aws_sns_topic.notifications.arn]
}

# The handler catches its own failures and reports them to SQS as batch
# item failures instead of raising, so the Lambda "Errors" metric above
# only fires for crashes/timeouts. This filter counts the structured
# ERROR log lines the handler emits for every failed attempt.
resource "aws_cloudwatch_log_metric_filter" "handler_errors" {
  name           = "${var.project_name}-handler-error-logs"
  log_group_name = aws_cloudwatch_log_group.lambda.name
  pattern        = "{ $.level = \"ERROR\" }"

  metric_transformation {
    name          = "HandlerErrorLogs"
    namespace     = "CsvSalesPipeline"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "handler_error_logs" {
  alarm_name          = "${var.project_name}-handler-error-logs"
  alarm_description   = "The handler logged one or more ERROR lines (a failed processing attempt, SNS failure, or malformed event). Filter the log group by level=ERROR to find the job_id."
  namespace           = "CsvSalesPipeline"
  metric_name         = "HandlerErrorLogs"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.notifications.arn]

  depends_on = [aws_cloudwatch_log_metric_filter.handler_errors]
}

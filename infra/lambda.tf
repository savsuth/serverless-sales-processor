# Reproducible packaging: this zips the exact contents of src/, which
# holds only the file_pipeline package (stdlib + boto3, and boto3 ships
# built into every AWS Lambda Python runtime, so nothing needs to be
# vendored or pip-installed into the archive). archive_file's hash drives
# Terraform's update-detection, so any source change produces a new
# deployment automatically. If you later add a third-party dependency
# beyond boto3, you'll need to switch to a build step (pip install
# --target + zip, or a Lambda layer) instead of zipping src/ directly;
# docs/decisions/0007 and 0010 describe what this constraint shapes.
data "archive_file" "lambda_package" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/lambda/build/file_pipeline.zip"
  excludes    = ["**/__pycache__", "**/*.pyc", "*.egg-info", "*.egg-info/**"]
}

resource "aws_lambda_function" "processor" {
  function_name = "${var.project_name}-processor"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "file_pipeline.handler.lambda_handler"
  runtime       = "python3.12"
  architectures = ["arm64"] # pure Python; arm64 is cheaper per GB-second
  timeout       = var.lambda_timeout_seconds
  memory_size   = var.lambda_memory_mb

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  # Concurrency is capped on the SQS event source mapping below, NOT with
  # reserved_concurrent_executions: when reserved concurrency throttles an
  # SQS-triggered function, throttled messages go back to the queue and
  # each attempt counts toward maxReceiveCount, so healthy messages can
  # end up in the DLQ. Reserved concurrency also fails outright on new
  # accounts whose total limit is at the 10-execution minimum.

  environment {
    variables = {
      JOB_TABLE_NAME         = aws_dynamodb_table.jobs.name
      OUTPUT_BUCKET_NAME     = aws_s3_bucket.output.id
      NOTIFICATION_TOPIC_ARN = aws_sns_topic.notifications.arn
      LEASE_SECONDS          = tostring(var.job_lease_seconds)
      MAX_INPUT_BYTES        = tostring(var.max_input_bytes)
      SCHEMA_NAME            = var.schema_name
      MAX_RECEIVE_COUNT      = tostring(var.sqs_max_receive_count)
      LOG_LEVEL              = "INFO"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda,
    aws_iam_role_policy.lambda_exec,
  ]
}

resource "aws_lambda_event_source_mapping" "processing_queue" {
  event_source_arn = aws_sqs_queue.processing.arn
  function_name    = aws_lambda_function.processor.arn
  batch_size       = var.sqs_batch_size

  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = var.lambda_max_concurrency
  }

  # Lambda validates the role's SQS permissions when the mapping is
  # created, so the policy must already be attached.
  depends_on = [aws_iam_role_policy.lambda_exec]
}

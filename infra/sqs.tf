# Standard (not FIFO) queue: this is a direct S3 event notification
# target, and S3 bucket notifications only support standard SQS queues.
#
# Visibility timeout follows AWS's documented Lambda/SQS guidance: set
# the queue's visibility timeout to at least 6x the function timeout, so
# a message can't become visible again (and get redelivered to a second
# concurrent invocation) while the first invocation might legitimately
# still be running. 6 * lambda_timeout_seconds is the floor; we use
# exactly that.

locals {
  sqs_visibility_timeout_seconds = var.lambda_timeout_seconds * 6
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project_name}-dlq"
  message_retention_seconds = var.dlq_message_retention_seconds
  sqs_managed_sse_enabled   = var.enable_kms ? null : true
  kms_master_key_id         = local.kms_key_arn
}

resource "aws_sqs_queue" "processing" {
  name                       = "${var.project_name}-processing"
  visibility_timeout_seconds = local.sqs_visibility_timeout_seconds
  message_retention_seconds  = var.sqs_message_retention_seconds
  sqs_managed_sse_enabled    = var.enable_kms ? null : true
  kms_master_key_id          = local.kms_key_arn

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
}

# Lets the DLQ's own redrive-allow policy confirm which queue may redrive
# *into* it (informational/AWS console clarity; also required by some
# organizations' SCPs that check redrive_allow_policy is set).
resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.processing.arn]
  })
}

data "aws_iam_policy_document" "processing_queue_policy" {
  statement {
    sid    = "AllowInputBucketNotifications"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.processing.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.input.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sqs_queue_policy" "processing" {
  queue_url = aws_sqs_queue.processing.id
  policy    = data.aws_iam_policy_document.processing_queue_policy.json
}

# One customer-managed key for the stack's data at rest: S3 buckets, SQS
# queues, the DynamoDB table, the SNS topic, CloudWatch log groups,
# Lambda environment variables, and Athena query results.
#
# Key administration is delegated to IAM in this account (the root
# statement), so it can never become unmanageable. Three AWS services act
# on the stack's behalf and need their own statements -- without them the
# failure is silent:
#   * S3 sends upload events to the encrypted processing queue;
#   * CloudWatch alarms publish to the encrypted SNS topic;
#   * CloudWatch Logs writes the encrypted log groups.
# The Lambda roles get key access through their own IAM policies.
#
# enable_kms = false keeps AWS-managed encryption everywhere (SSE-S3,
# SQS-managed SSE, the DynamoDB-owned key, and so on).

locals {
  kms_key_arn = var.enable_kms ? aws_kms_key.main[0].arn : null
  account_id  = data.aws_caller_identity.current.account_id
}

data "aws_iam_policy_document" "kms" {
  #checkov:skip=CKV_AWS_109:Key policy: resources "*" means this key only.
  #checkov:skip=CKV_AWS_111:Key policy: resources "*" means this key only.
  #checkov:skip=CKV_AWS_356:Key policy: resources "*" means this key only.
  count = var.enable_kms ? 1 : 0

  statement {
    sid       = "AccountAdministersKeyThroughIAM"
    effect    = "Allow"
    actions   = ["kms:*"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }
  }

  statement {
    sid       = "S3SendsUploadEventsToEncryptedQueue"
    effect    = "Allow"
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = ["*"]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.input.arn]
    }
  }

  statement {
    sid       = "CloudWatchAlarmsPublishToEncryptedTopic"
    effect    = "Allow"
    actions   = ["kms:GenerateDataKey*", "kms:Decrypt"]
    resources = ["*"]

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }

  statement {
    sid    = "CloudWatchLogsEncryptsProjectLogGroups"
    effect = "Allow"
    actions = [
      "kms:Encrypt*",
      "kms:Decrypt*",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:Describe*",
    ]
    resources = ["*"]

    principals {
      type        = "Service"
      identifiers = ["logs.${var.aws_region}.amazonaws.com"]
    }

    condition {
      test     = "ArnLike"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = ["arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:*"]
    }
  }
}

resource "aws_kms_key" "main" {
  count                   = var.enable_kms ? 1 : 0
  description             = "${var.project_name}: data at rest (S3, SQS, DynamoDB, SNS, logs, Lambda settings)"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.kms[0].json
}

resource "aws_kms_alias" "main" {
  count         = var.enable_kms ? 1 : 0
  name          = "alias/${var.project_name}"
  target_key_id = aws_kms_key.main[0].key_id
}

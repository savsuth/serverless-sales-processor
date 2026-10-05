# Every statement below is scoped to a specific resource ARN this
# project created -- there are no wildcard resource grants in this
# policy. (The one wildcard suffix you'll see, ":*" appended to the log
# group ARN, is not "all log groups" -- it means "all log streams within
# this one log group", which is how CloudWatch Logs ARNs address streams.)

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${var.project_name}-lambda-exec"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "lambda_permissions" {
  statement {
    sid       = "ReadInputObjectVersions"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.input.arn}/*"]
  }

  statement {
    sid     = "WriteOutputReports"
    effect  = "Allow"
    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.output.arn}/reports/*",
      "${aws_s3_bucket.output.arn}/curated/*",
    ]
  }

  statement {
    sid    = "ConsumeProcessingQueue"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.processing.arn]
  }

  statement {
    sid       = "ReadWriteJobTable"
    effect    = "Allow"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.jobs.arn]
  }

  statement {
    sid       = "PublishJobNotifications"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.notifications.arn]
  }

  statement {
    sid    = "WriteOwnLogGroup"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      aws_cloudwatch_log_group.lambda.arn,
      "${aws_cloudwatch_log_group.lambda.arn}:*",
    ]
  }
}

resource "aws_iam_role_policy" "lambda_exec" {
  name   = "${var.project_name}-lambda-permissions"
  role   = aws_iam_role.lambda_exec.id
  policy = data.aws_iam_policy_document.lambda_permissions.json
}

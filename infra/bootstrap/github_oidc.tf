# Optional GitHub Actions deploy role. No AWS access keys are stored in
# GitHub: Actions exchanges a short-lived OIDC token for temporary
# credentials via sts:AssumeRoleWithWebIdentity.
#
# This lives in the bootstrap root (applied from your laptop) rather than
# the main stack so the deploy role never manages -- and so can never
# rewrite -- its own permissions.

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = var.aws_region
  prefix     = var.project_name
  # Glue database names cannot contain hyphens; matches infra/athena.tf.
  glue_database = replace(var.project_name, "-", "_")

  create_oidc_provider = var.enable_github_oidc && var.existing_github_oidc_provider_arn == ""
  oidc_provider_arn = var.existing_github_oidc_provider_arn != "" ? var.existing_github_oidc_provider_arn : (
    local.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : ""
  )

  # For a job that declares `environment: production`, GitHub's OIDC
  # subject is repo:OWNER/REPO:environment:production (the branch is NOT
  # in the subject). The branch restriction therefore lives on the GitHub
  # Environment's deployment-branch rule -- see README.
  github_oidc_sub = "repo:${var.github_repository}:environment:${var.github_environment}"
}

resource "aws_iam_openid_connect_provider" "github" {
  count = local.create_oidc_provider ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # SHA1 thumbprints of the CA certs in token.actions.githubusercontent.com's
  # TLS chain (root-most first), captured directly via
  # `openssl s_client -connect token.actions.githubusercontent.com:443
  # -showcerts` rather than copied from a doc, since GitHub has changed
  # CAs before (it now serves a Let's Encrypt chain). AWS no longer
  # strictly validates this value for well-known CAs, but Terraform
  # requires a correctly-shaped one. Re-run that command and update this
  # list if GitHub rotates CAs again.
  thumbprint_list = [
    "ab9d0263244dd0326eb67015705a667e79cfe998",
    "2d74d6dfd96eea55ad7baafa0d3c6552b2dadc37",
  ]
}

data "aws_iam_policy_document" "github_actions_assume_role" {
  count = var.enable_github_oidc ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # StringEquals, not StringLike: no wildcard anywhere in the subject.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [local.github_oidc_sub]
    }
  }
}

resource "aws_iam_role" "github_actions_deploy" {
  count = var.enable_github_oidc ? 1 : 0

  name                 = "${var.project_name}-github-actions-deploy"
  assume_role_policy   = data.aws_iam_policy_document.github_actions_assume_role[0].json
  max_session_duration = 3600

  lifecycle {
    precondition {
      condition     = var.github_repository != ""
      error_message = "github_repository is required when enable_github_oidc is true."
    }
  }
}

# Deliberately service-scoped rather than AdministratorAccess. Every
# statement is limited to this project's name prefix; the only wildcard
# resources are the ones AWS gives no way to scope, each marked below.
data "aws_iam_policy_document" "github_actions_deploy" {
  count = var.enable_github_oidc ? 1 : 0

  # Remote state: read/write the one state object and its lock file, and
  # list only under that key. Cannot touch other objects or delete the bucket.
  statement {
    sid       = "StateBucketList"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.state.arn]

    condition {
      test     = "StringEquals"
      variable = "s3:prefix"
      values   = [var.state_key, "${var.state_key}.tflock"]
    }
  }

  statement {
    sid       = "StateObjectReadWrite"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.state.arn}/${var.state_key}", "${aws_s3_bucket.state.arn}/${var.state_key}.tflock"]
  }

  # Data buckets: s3:* on the two project buckets only (the tfstate bucket
  # is intentionally NOT matched). Terraform reads a dozen bucket sub-
  # resources (encryption, lifecycle, accelerate, ...) with differently
  # named actions, so a name-pattern list here silently misses some.
  statement {
    sid     = "ManageDataBuckets"
    effect  = "Allow"
    actions = ["s3:*"]
    resources = [
      "arn:aws:s3:::${local.prefix}-input-*",
      "arn:aws:s3:::${local.prefix}-input-*/*",
      "arn:aws:s3:::${local.prefix}-output-*",
      "arn:aws:s3:::${local.prefix}-output-*/*",
      "arn:aws:s3:::${local.prefix}-athena-results-*",
      "arn:aws:s3:::${local.prefix}-athena-results-*/*",
    ]
  }

  # The Glue database and table behind Athena. The catalog ARN is required
  # alongside the database for any database-level call.
  statement {
    sid     = "ManageGlueCatalog"
    effect  = "Allow"
    actions = ["glue:*"]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/${local.glue_database}",
      "arn:aws:glue:${local.region}:${local.account_id}:table/${local.glue_database}/*",
    ]
  }

  # The Athena workgroup and its saved queries (named queries are
  # authorized against the workgroup ARN).
  statement {
    sid       = "ManageAthenaWorkgroup"
    effect    = "Allow"
    actions   = ["athena:*"]
    resources = ["arn:aws:athena:${local.region}:${local.account_id}:workgroup/${local.prefix}-*"]
  }

  statement {
    sid       = "ManageQueues"
    effect    = "Allow"
    actions   = ["sqs:*"]
    resources = ["arn:aws:sqs:${local.region}:${local.account_id}:${local.prefix}-*"]
  }

  statement {
    sid       = "ManageJobTable"
    effect    = "Allow"
    actions   = ["dynamodb:*"]
    resources = ["arn:aws:dynamodb:${local.region}:${local.account_id}:table/${local.prefix}-*"]
  }

  statement {
    sid       = "ManageTopic"
    effect    = "Allow"
    actions   = ["sns:*"]
    resources = ["arn:aws:sns:${local.region}:${local.account_id}:${local.prefix}-*"]
  }

  # Unavoidable wildcard #1: an event source mapping's ARN contains a
  # random UUID that does not exist until it is created.
  statement {
    sid     = "ManageLambda"
    effect  = "Allow"
    actions = ["lambda:*"]
    resources = [
      "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.prefix}-*",
      "arn:aws:lambda:${local.region}:${local.account_id}:event-source-mapping:*",
    ]
  }

  # Unavoidable wildcard #2: logs:DescribeLogGroups is a list call that
  # ignores resource-level scoping, and Terraform calls it to read the group.
  statement {
    sid       = "DescribeLogGroups"
    effect    = "Allow"
    actions   = ["logs:DescribeLogGroups"]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:log-group::log-stream:", "arn:aws:logs:${local.region}:${local.account_id}:log-group:*"]
  }

  statement {
    sid     = "ManageLogGroup"
    effect  = "Allow"
    actions = ["logs:*"]
    resources = [
      "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/${local.prefix}-*",
      "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/${local.prefix}-*:*",
    ]
  }

  statement {
    sid    = "ManageAlarms"
    effect = "Allow"
    actions = [
      "cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms",
      "cloudwatch:ListTagsForResource", "cloudwatch:TagResource", "cloudwatch:UntagResource",
    ]
    resources = ["arn:aws:cloudwatch:${local.region}:${local.account_id}:alarm:${local.prefix}-*"]
  }

  # Unavoidable wildcard #3: DescribeAlarms does not support resource-level
  # permissions.
  statement {
    sid       = "DescribeAlarms"
    effect    = "Allow"
    actions   = ["cloudwatch:DescribeAlarms"]
    resources = ["*"]
  }

  statement {
    sid       = "ManageBudget"
    effect    = "Allow"
    actions   = ["budgets:ViewBudget", "budgets:ModifyBudget"]
    resources = ["arn:aws:budgets::${local.account_id}:budget/${local.prefix}-*"]
  }

  # IAM is limited to the ONE role name the Lambda uses -- not a prefix --
  # so this role (named ...-github-actions-deploy) cannot edit itself,
  # which would otherwise be a path to full admin.
  statement {
    sid    = "ManageLambdaExecutionRole"
    effect = "Allow"
    actions = [
      "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:TagRole", "iam:UntagRole",
      "iam:UpdateAssumeRolePolicy", "iam:PutRolePolicy", "iam:DeleteRolePolicy",
      "iam:GetRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole",
    ]
    resources = ["arn:aws:iam::${local.account_id}:role/${local.prefix}-lambda-exec"]
  }

  statement {
    sid       = "PassExecutionRoleToLambdaOnly"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = ["arn:aws:iam::${local.account_id}:role/${local.prefix}-lambda-exec"]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "github_actions_deploy" {
  count = var.enable_github_oidc ? 1 : 0

  name   = "${var.project_name}-github-actions-deploy-permissions"
  role   = aws_iam_role.github_actions_deploy[0].id
  policy = data.aws_iam_policy_document.github_actions_deploy[0].json
}

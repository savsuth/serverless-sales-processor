# The portal: a small token-protected API (src/file_pipeline/portal.py)
# behind an API Gateway HTTP API, used by the upload page
# (tools/upload.html) and by the signed report links in notification
# emails. API Gateway, not a public Lambda function URL: only API Gateway
# may invoke the function, and it rate-limits requests.

resource "random_password" "upload_token" {
  count   = var.enable_portal ? 1 : 0
  length  = 40
  special = false
}

resource "random_password" "link_signing_key" {
  count   = var.enable_portal ? 1 : 0
  length  = 64
  special = false
}

locals {
  portal_url       = var.enable_portal ? aws_apigatewayv2_api.portal[0].api_endpoint : ""
  link_signing_key = var.enable_portal ? random_password.link_signing_key[0].result : ""
}

# --- Function ------------------------------------------------------------

data "aws_iam_policy_document" "portal_permissions" {
  count = var.enable_portal ? 1 : 0

  # Presigned POSTs are authorized as this role, so it may create new
  # objects under uploads/ only.
  statement {
    sid       = "CreateUploads"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.input.arn}/uploads/*"]
  }

  statement {
    sid       = "ReadReports"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.output.arn}/reports/*"]
  }

  statement {
    sid       = "ReadJobs"
    effect    = "Allow"
    actions   = ["dynamodb:GetItem"]
    resources = [aws_dynamodb_table.jobs.arn]
  }

  statement {
    sid       = "WriteOwnLogGroup"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.portal[0].arn}:*"]
  }
}

resource "aws_iam_role" "portal" {
  count              = var.enable_portal ? 1 : 0
  name               = "${var.project_name}-portal"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy" "portal" {
  count  = var.enable_portal ? 1 : 0
  name   = "${var.project_name}-portal-permissions"
  role   = aws_iam_role.portal[0].id
  policy = data.aws_iam_policy_document.portal_permissions[0].json
}

resource "aws_cloudwatch_log_group" "portal" {
  count             = var.enable_portal ? 1 : 0
  name              = "/aws/lambda/${var.project_name}-portal"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "portal" {
  count         = var.enable_portal ? 1 : 0
  function_name = "${var.project_name}-portal"
  role          = aws_iam_role.portal[0].arn
  handler       = "file_pipeline.portal.lambda_handler"
  runtime       = "python3.12"
  architectures = ["arm64"]
  timeout       = 10
  memory_size   = 256

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  environment {
    variables = {
      JOB_TABLE_NAME     = aws_dynamodb_table.jobs.name
      INPUT_BUCKET_NAME  = aws_s3_bucket.input.id
      OUTPUT_BUCKET_NAME = aws_s3_bucket.output.id
      MAX_INPUT_BYTES    = tostring(var.max_input_bytes)
      # Only the token's SHA-256 is given to the function.
      UPLOAD_TOKEN_SHA256 = sha256(random_password.upload_token[0].result)
      LINK_SIGNING_KEY    = random_password.link_signing_key[0].result
    }
  }

  depends_on = [aws_cloudwatch_log_group.portal, aws_iam_role_policy.portal]
}

# --- HTTP API --------------------------------------------------------------

resource "aws_apigatewayv2_api" "portal" {
  count         = var.enable_portal ? 1 : 0
  name          = "${var.project_name}-portal"
  protocol_type = "HTTP"
  description   = "Upload page and signed report links for ${var.project_name}."

  # The upload page is a local file, so its origin is "null": allow any
  # origin. Every route still needs the token or a valid signature.
  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "POST"]
    allow_headers = ["authorization", "content-type"]
    max_age       = 300
  }
}

resource "aws_apigatewayv2_integration" "portal" {
  count                  = var.enable_portal ? 1 : 0
  api_id                 = aws_apigatewayv2_api.portal[0].id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.portal[0].invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "portal" {
  for_each = var.enable_portal ? toset([
    "POST /uploads",
    "GET /jobs",
    "GET /jobs/{job_id}",
    "GET /r/{job_id}/{file}",
  ]) : toset([])

  api_id    = aws_apigatewayv2_api.portal[0].id
  route_key = each.value
  target    = "integrations/${aws_apigatewayv2_integration.portal[0].id}"
}

resource "aws_cloudwatch_log_group" "portal_access" {
  count             = var.enable_portal ? 1 : 0
  name              = "/aws/apigateway/${var.project_name}-portal"
  retention_in_days = var.log_retention_days
}

resource "aws_apigatewayv2_stage" "portal" {
  count       = var.enable_portal ? 1 : 0
  api_id      = aws_apigatewayv2_api.portal[0].id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_rate_limit  = var.portal_requests_per_second
    throttling_burst_limit = var.portal_requests_per_second * 2
  }

  # Request metadata only; the query string (which carries link
  # signatures) and the authorization header are not logged.
  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.portal_access[0].arn
    format = jsonencode({
      requestId = "$context.requestId"
      time      = "$context.requestTime"
      method    = "$context.httpMethod"
      route     = "$context.routeKey"
      status    = "$context.status"
      sourceIp  = "$context.identity.sourceIp"
    })
  }
}

resource "aws_lambda_permission" "portal_api" {
  count         = var.enable_portal ? 1 : 0
  statement_id  = "AllowPortalApiGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.portal[0].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.portal[0].execution_arn}/*/*"
}

# --- Browser uploads to the input bucket ----------------------------------

resource "aws_s3_bucket_cors_configuration" "input" {
  count  = var.enable_portal ? 1 : 0
  bucket = aws_s3_bucket.input.id

  cors_rule {
    allowed_methods = ["POST"]
    allowed_origins = ["*"]
    allowed_headers = ["*"]
    # The page reads the new object's version to compute its job ID.
    expose_headers  = ["x-amz-version-id", "ETag"]
    max_age_seconds = 3000
  }
}

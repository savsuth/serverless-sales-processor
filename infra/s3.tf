# --- Input bucket -----------------------------------------------------
# Versioned so the exact uploaded object version can always be re-read
# even if the key is later overwritten, and so job identity (bucket + key
# + version ID) stays stable and unambiguous.

resource "aws_s3_bucket" "input" {
  bucket = "${var.project_name}-input-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "input" {
  bucket = aws_s3_bucket.input.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "input" {
  bucket                  = aws_s3_bucket.input.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "input" {
  bucket = aws_s3_bucket.input.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.enable_kms ? "aws:kms" : "AES256"
      kms_master_key_id = local.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

# Every new object is sent to the queue; the Lambda itself decides which
# keys are inputs (.csv or .csv.gz, any letter case -- see
# storage.is_input_key). S3's suffix filter is case-sensitive, so filtering
# here would silently skip uploads such as "Sales.CSV".
resource "aws_s3_bucket_notification" "input" {
  bucket = aws_s3_bucket.input.id

  queue {
    queue_arn = aws_sqs_queue.processing.arn
    events    = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_sqs_queue_policy.processing]
}

# --- Output bucket ------------------------------------------------------
# Reports are private (never public) -- notifications share s3:// paths
# for authorized users to fetch via the console or CLI, per the "keep
# reports private" requirement. Versioned too, so an overwritten report
# (a job retry) doesn't destroy the ability to inspect a prior attempt.

resource "aws_s3_bucket" "output" {
  bucket = "${var.project_name}-output-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "output" {
  bucket = aws_s3_bucket.output.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "output" {
  bucket                  = aws_s3_bucket.output.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "output" {
  bucket = aws_s3_bucket.output.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.enable_kms ? "aws:kms" : "AES256"
      kms_master_key_id = local.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

# --- Transport security and housekeeping (both buckets) -----------------

data "aws_iam_policy_document" "require_tls" {
  for_each = {
    input  = aws_s3_bucket.input.arn
    output = aws_s3_bucket.output.arn
  }

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      each.value,
      "${each.value}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "input" {
  bucket     = aws_s3_bucket.input.id
  policy     = data.aws_iam_policy_document.require_tls["input"].json
  depends_on = [aws_s3_bucket_public_access_block.input]
}

resource "aws_s3_bucket_policy" "output" {
  bucket     = aws_s3_bucket.output.id
  policy     = data.aws_iam_policy_document.require_tls["output"].json
  depends_on = [aws_s3_bucket_public_access_block.output]
}

# Always cleans up abandoned multipart-upload fragments (invisible but
# billed). Uploads expire only if upload_retention_days is set; this
# project never deletes data unless asked to. Outputs never expire.
# Expiring a current version adds a delete marker and makes it
# noncurrent, so noncurrent versions expire after the same period: an
# upload is kept at least that long, and an overwritten one too.
resource "aws_s3_bucket_lifecycle_configuration" "abort_incomplete_uploads" {
  for_each = {
    input  = aws_s3_bucket.input.id
    output = aws_s3_bucket.output.id
  }

  bucket = each.value

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  dynamic "rule" {
    for_each = each.key == "input" && var.upload_retention_days > 0 ? [var.upload_retention_days] : []
    content {
      id     = "expire-uploads"
      status = "Enabled"

      filter {}

      expiration {
        days = rule.value
      }

      noncurrent_version_expiration {
        noncurrent_days = rule.value
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.input, aws_s3_bucket_versioning.output]
}

# Queryable history: the Lambda writes every valid row of each completed
# job to curated/<schema>/month=YYYY-MM/<job_id>.json.gz in the output
# bucket (see src/file_pipeline/curated.py). This file makes that data a
# SQL table in Athena.
#
# Partition projection: Athena computes the month partitions from the
# table properties below instead of a crawler or MSCK REPAIR, so a new
# month is queryable the moment its first file lands, with nothing to
# run or pay for. Rows dated outside athena_month_range are stored but
# not visible to queries until the range is widened.

locals {
  glue_database_name = replace(var.project_name, "-", "_")
  curated_location   = "s3://${aws_s3_bucket.output.id}/curated/${local.schema.name}"
  example_query_dir  = "${path.module}/../docs/queries/${local.schema.name}"
}

resource "aws_glue_catalog_database" "analytics" {
  count = local.athena_enabled ? 1 : 0

  name        = local.glue_database_name
  description = "Curated data written by the ${var.project_name} Lambda."
}

resource "aws_glue_catalog_table" "curated" {
  count = local.athena_enabled ? 1 : 0

  name          = local.schema.name
  database_name = aws_glue_catalog_database.analytics[0].name
  description   = "Every valid row of every completed ${local.schema.name} job, one row per source CSV row."
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                         = "TRUE"
    classification                   = "json"
    compressionType                  = "gzip"
    "projection.enabled"             = "true"
    "projection.month.type"          = "date"
    "projection.month.format"        = "yyyy-MM"
    "projection.month.range"         = var.athena_month_range
    "projection.month.interval"      = "1"
    "projection.month.interval.unit" = "MONTHS"
    "storage.location.template"      = "${local.curated_location}/month=$${month}/"
  }

  partition_keys {
    name = "month"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.curated_location}/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hive.hcatalog.data.JsonSerDe"
    }

    dynamic "columns" {
      for_each = local.curated_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# --- Query results -------------------------------------------------------
# Athena writes every query's result set to S3. These are disposable
# copies of query output, not project data, so unlike the input and
# output buckets this one expires its objects.

resource "aws_s3_bucket" "athena_results" {
  #checkov:skip=CKV_AWS_21:Query results are disposable copies, expired after a few days.
  count = local.athena_enabled ? 1 : 0

  bucket = "${var.project_name}-athena-results-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  count = local.athena_enabled ? 1 : 0

  bucket                  = aws_s3_bucket.athena_results[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  count = local.athena_enabled ? 1 : 0

  bucket = aws_s3_bucket.athena_results[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.enable_kms ? "aws:kms" : "AES256"
      kms_master_key_id = local.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

data "aws_iam_policy_document" "athena_results_require_tls" {
  count = local.athena_enabled ? 1 : 0

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.athena_results[0].arn,
      "${aws_s3_bucket.athena_results[0].arn}/*",
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

resource "aws_s3_bucket_policy" "athena_results" {
  count = local.athena_enabled ? 1 : 0

  bucket     = aws_s3_bucket.athena_results[0].id
  policy     = data.aws_iam_policy_document.athena_results_require_tls[0].json
  depends_on = [aws_s3_bucket_public_access_block.athena_results]
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  count = local.athena_enabled ? 1 : 0

  bucket = aws_s3_bucket.athena_results[0].id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    expiration {
      days = var.athena_results_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# --- Workgroup ------------------------------------------------------------
# Queries run in this workgroup are forced to use its settings: results
# go to the bucket above, encrypted, and any single query that would scan
# more than athena_bytes_scanned_cutoff is cancelled -- a cost guard, since
# Athena bills per byte scanned.

resource "aws_athena_workgroup" "analytics" {
  count = local.athena_enabled ? 1 : 0

  name        = "${var.project_name}-analytics"
  description = "Queries over the ${local.schema.name} curated data."

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results[0].id}/results/"

      encryption_configuration {
        encryption_option = var.enable_kms ? "SSE_KMS" : "SSE_S3"
        kms_key_arn       = local.kms_key_arn
      }
    }
  }
}

# Each docs/queries/<schema>/*.sql file becomes a saved query in the
# workgroup, so the examples are one click away in the Athena console.
resource "aws_athena_named_query" "examples" {
  for_each = local.athena_enabled ? fileset(local.example_query_dir, "*.sql") : toset([])

  name      = trimsuffix(each.value, ".sql")
  workgroup = aws_athena_workgroup.analytics[0].id
  database  = aws_glue_catalog_database.analytics[0].name
  query     = file("${local.example_query_dir}/${each.value}")
}

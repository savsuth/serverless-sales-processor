# job_id (sha256 of bucket+key+versionId) is globally unique and is the
# only lookup pattern the handler needs; the index below serves operators.
# On-demand billing avoids needing to guess throughput for a workload
# that is, by nature, bursty and driven by upload volume.

resource "aws_dynamodb_table" "jobs" {
  name         = "${var.project_name}-jobs"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "job_id"

  attribute {
    name = "job_id"
    type = "S"
  }

  attribute {
    name = "status"
    type = "S"
  }

  attribute {
    name = "created_at"
    type = "S"
  }

  # Lists jobs by status, newest first (scripts/list_jobs.sh,
  # scripts/reprocess.py --status). Content fingerprint items have no
  # status, so they never appear in it.
  global_secondary_index {
    name            = "status-created_at-index"
    hash_key        = "status"
    range_key       = "created_at"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }
}

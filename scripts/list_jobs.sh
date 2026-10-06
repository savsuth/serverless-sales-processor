#!/usr/bin/env bash
# Lists jobs with a given status, newest first, using the job table's
# status index (no table scan).
#
# Usage: scripts/list_jobs.sh <job-table-name> <status> [max-items]
#   status: completed, completed_with_rejections, validation_failed,
#           duplicate_content, dead_lettered, failed, processing
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <job-table-name> <status> [max-items]" >&2
  exit 1
fi

aws dynamodb query \
  --table-name "$1" \
  --index-name status-created_at-index \
  --key-condition-expression "#s = :s" \
  --expression-attribute-names '{"#s": "status"}' \
  --expression-attribute-values "{\":s\": {\"S\": \"$2\"}}" \
  --no-scan-index-forward \
  --max-items "${3:-25}" \
  --query 'Items[].[created_at.S, job_id.S, error_code.S, source_key.S]' \
  --output table

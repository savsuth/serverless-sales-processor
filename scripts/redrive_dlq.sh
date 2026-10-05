#!/usr/bin/env bash
# Moves every message currently in the dead-letter queue back onto the
# main processing queue for another attempt, using SQS's native message
# move task (no manual receive/send/delete loop, and it's safe to run
# even if the DLQ is empty).
#
# Before running this: inspect *why* the messages failed and fix the
# underlying cause first, or you'll just refill the DLQ. Each message body
# is the original S3 event (`aws sqs receive-message --queue-url <dlq-url>`);
# its job's error_code is on the job record (scripts/check_job.sh).
#
# Usage: scripts/redrive_dlq.sh <dlq-url> <processing-queue-arn>
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <dlq-url> <processing-queue-arn>" >&2
  exit 1
fi

DLQ_URL="$1"
DEST_QUEUE_ARN="$2"

DLQ_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --query "Attributes.QueueArn" --output text)

echo "Starting message move task: $DLQ_ARN -> $DEST_QUEUE_ARN" >&2
aws sqs start-message-move-task \
  --source-arn "$DLQ_ARN" \
  --destination-arn "$DEST_QUEUE_ARN"

echo
echo "Track progress with:" >&2
echo "  aws sqs list-message-move-tasks --source-arn $DLQ_ARN" >&2

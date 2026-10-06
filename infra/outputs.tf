output "input_bucket_name" {
  description = "Upload sales CSVs here to trigger processing."
  value       = aws_s3_bucket.input.id
}

output "output_bucket_name" {
  description = "Reports are written to reports/{job_id}/ in this (private) bucket."
  value       = aws_s3_bucket.output.id
}

output "job_table_name" {
  description = "DynamoDB table tracking per-job status. Query with the job_id shown in each SNS notification."
  value       = aws_dynamodb_table.jobs.name
}

output "processing_queue_url" {
  description = "Main SQS queue between S3 and Lambda."
  value       = aws_sqs_queue.processing.id
}

output "processing_queue_arn" {
  description = "Destination ARN for scripts/redrive_dlq.sh."
  value       = aws_sqs_queue.processing.arn
}

output "dead_letter_queue_url" {
  description = "Messages that exhausted all retries land here. Fix the cause, then move them back with scripts/redrive_dlq.sh."
  value       = aws_sqs_queue.dlq.id
}

output "notification_topic_arn" {
  description = "SNS topic for job completion notifications and CloudWatch alarms."
  value       = aws_sns_topic.notifications.arn
}

output "lambda_function_name" {
  description = "Use with `aws logs tail` / CloudWatch Logs Insights to find a job's logs."
  value       = aws_lambda_function.processor.function_name
}

output "lambda_log_group_name" {
  value = aws_cloudwatch_log_group.lambda.name
}

output "athena_workgroup_name" {
  description = "Run queries in this workgroup (Athena console: choose it at the top of the query editor). Example queries are saved in it."
  value       = try(aws_athena_workgroup.analytics[0].name, null)
}

output "athena_database_name" {
  description = "Glue database holding the curated table."
  value       = try(aws_glue_catalog_database.analytics[0].name, null)
}

output "athena_table_name" {
  description = "Curated table: one row per valid source CSV row of every completed job, partitioned by month."
  value       = try(aws_glue_catalog_table.curated[0].name, null)
}

output "job_status_index_name" {
  description = "DynamoDB index for listing jobs by status (scripts/list_jobs.sh)."
  value       = "status-created_at-index"
}

output "portal_url" {
  description = "Paste into tools/upload.html along with the upload token."
  value       = local.portal_url
}

output "upload_token" {
  description = "Secret for tools/upload.html: terraform output -raw upload_token"
  value       = var.enable_portal ? random_password.upload_token[0].result : null
  sensitive   = true
}

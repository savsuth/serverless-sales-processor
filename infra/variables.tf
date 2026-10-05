variable "project_name" {
  description = "Prefix applied to all resource names, so multiple copies (dev/staging) can coexist."
  type        = string
  default     = "csv-sales-pipeline"
}

variable "aws_region" {
  description = "Region for every resource. The S3 bucket and its SQS notification queue must share a region, so this is the single region for the whole stack. Defaults to us-east-2: this AWS account has an organization-level Service Control Policy (AdvancedModeRegionRestrictionSecurityControlPolicy) that funnels general workloads to us-east-2 and denies most S3/EC2/DynamoDB/Lambda actions in us-east-1 and us-west-2. Override only if your account doesn't have that restriction."
  type        = string
  default     = "us-east-2"
}

variable "max_input_bytes" {
  description = "Maximum accepted CSV size in bytes. Enforced by the Lambda before it downloads the object body (via HeadObject) and again while streaming, matching the same limit used by the local CLI runner."
  type        = number
  default     = 10485760 # 10 MiB
}

variable "schema_name" {
  description = "Which bundled schema (src/file_pipeline/schemas/<name>.json) the Lambda validates and aggregates uploads with. One schema per deployment. Also names the Athena table."
  type        = string
  default     = "sales"
}

# --- Athena (queryable history) ---------------------------------------

variable "enable_athena" {
  description = "Create the Glue table, Athena workgroup, and query-results bucket for the curated data. Takes effect only when the schema has a \"curated\" section; the Lambda writes curated files either way."
  type        = bool
  default     = true
}

variable "athena_bytes_scanned_cutoff" {
  description = "Athena cancels any single query in the project workgroup that would scan more than this many bytes (Athena bills per byte scanned). AWS minimum is 10 MB."
  type        = number
  default     = 1073741824 # 1 GiB

  validation {
    condition     = var.athena_bytes_scanned_cutoff >= 10485760
    error_message = "athena_bytes_scanned_cutoff must be at least 10485760 (10 MB), the AWS minimum."
  }
}

variable "athena_results_retention_days" {
  description = "Days before Athena query-result files are deleted from the results bucket. They are copies of query output, not project data."
  type        = number
  default     = 7
}

variable "athena_month_range" {
  description = "Months Athena's partition projection exposes, as \"FIRST,LAST\" in yyyy-MM (\"NOW\" = the current month). Curated rows dated outside the range are stored but not visible to queries."
  type        = string
  default     = "2000-01,NOW"
}

# --- Lambda sizing ---------------------------------------------------------
# Conservative defaults for a CSV up to 10 MiB. Revenue/quantity aggregation
# scales with distinct product count (held in memory), not row count, and
# rows themselves are streamed -- so memory needs are modest even for a
# file with a lot of rows, as long as the product cardinality stays sane.

variable "lambda_timeout_seconds" {
  description = "Lambda function timeout. 60s comfortably covers parsing/aggregating a 10 MiB CSV plus S3/DynamoDB/SNS round trips with margin."
  type        = number
  default     = 60
}

variable "lambda_memory_mb" {
  description = "Lambda memory. 512 MB gives headroom for Decimal-heavy aggregation and CPython overhead without over-provisioning for a bounded 10 MiB input."
  type        = number
  default     = 512
}

variable "lambda_max_concurrency" {
  description = "Maximum concurrent Lambda invocations the SQS event source mapping will run (AWS allows 2-1000). Bounds parallel S3/DynamoDB/SNS load without the throttle-and-redeliver problem reserved concurrency causes for SQS triggers."
  type        = number
  default     = 5

  validation {
    condition     = var.lambda_max_concurrency >= 2 && var.lambda_max_concurrency <= 1000
    error_message = "lambda_max_concurrency must be between 2 and 1000 (an AWS event-source-mapping limit)."
  }
}

variable "sqs_batch_size" {
  description = "Max SQS messages per Lambda invocation. Kept at 1: each CSV's processing cost varies with its row/product cardinality, and batching multiple heavy files into a single invocation risks starving later items of the shared function-timeout budget. Revisit once per-file cost is well understood."
  type        = number
  default     = 1
}

variable "sqs_max_receive_count" {
  description = "How many times a message is retried (redelivered after its visibility timeout) before SQS moves it to the dead-letter queue."
  type        = number
  default     = 5
}

variable "sqs_message_retention_seconds" {
  description = "How long the main processing queue retains an unprocessed message."
  type        = number
  default     = 345600 # 4 days (SQS default)
}

variable "dlq_message_retention_seconds" {
  description = "How long the dead-letter queue retains a failed message before investigation. Kept at the SQS maximum so a failure over a long weekend is never silently lost."
  type        = number
  default     = 1209600 # 14 days (SQS maximum)
}

variable "job_lease_seconds" {
  description = "DynamoDB processing-claim lease duration. Kept comfortably above lambda_timeout_seconds so a legitimately-still-running invocation is never treated as crashed, while still recovering promptly if one actually does crash."
  type        = number
  default     = 120
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for the Lambda's log group."
  type        = number
  default     = 30
}

# --- Notifications -----------------------------------------------------

variable "enable_events" {
  description = "Publish a \"CSV job finished\" / \"CSV job dead-lettered\" event to the account's default EventBridge bus for every job outcome, so other systems can subscribe with an EventBridge rule (source = project_name)."
  type        = bool
  default     = true
}

variable "notification_email" {
  description = "Optional email address to subscribe to the SNS notification topic (job completion + alarms). Leave empty to create the topic without a subscription and add one later. AWS requires the recipient to confirm the subscription (a confirmation email is sent) before delivery starts."
  type        = string
  default     = ""
}

# --- Portal: upload page and report links -----------------------------

variable "enable_portal" {
  description = "Create the token-protected portal API used by tools/upload.html and by the download links in notification emails."
  type        = bool
  default     = true
}

variable "report_link_days" {
  description = "How long the download links in notification emails stay valid."
  type        = number
  default     = 7
}

variable "portal_requests_per_second" {
  description = "API Gateway steady-state request limit for the portal (burst is twice this)."
  type        = number
  default     = 10
}

# --- Budget alert (optional) -------------------------------------------

variable "enable_budget_alert" {
  description = "Whether to create an AWS Budget alert for this project's estimated spend. A budget alert only sends a notification -- it never stops spending or enforces a cap."
  type        = bool
  default     = false
}

variable "budget_limit_usd" {
  description = "Monthly budget threshold in USD, used only when enable_budget_alert is true."
  type        = number
  default     = 10
}

variable "budget_alert_email" {
  description = "Email address to notify when the budget threshold is crossed, used only when enable_budget_alert is true."
  type        = string
  default     = ""
}

# Serverless Sales Processor

This project computes per-product revenue and quantity totals from a
sales CSV, using Decimal arithmetic to avoid floating-point rounding
error, and records every invalid row with a specific rejection reason
instead of failing the whole file over one bad line.

The validation and aggregation logic is implemented once and runs
unmodified in two environments: as a local command-line tool, and as an
AWS Lambda function triggered by S3 object-creation events. Each upload
is assigned a deterministic job ID derived from the object's bucket, key,
and version, claimed atomically in DynamoDB, and tracked through to a
terminal status and an SNS notification, so a retried or duplicated event
cannot silently reprocess a job that already completed. The same file
content uploaded twice is recognized and not counted again, and every
valid row of a completed job is written as queryable data for Amazon
Athena. Files can also be uploaded from a small token-protected web page,
and notification emails carry download links. The repository contains
the application, its test suite, and the Terraform configuration used to
deploy the AWS stack.

## Features

- Streaming CSV validation: required-column checks, per-row validation,
  and duplicate-header detection without loading the whole file into
  memory. `.csv` and `.csv.gz` in any letter case; comma-, semicolon- or
  tab-separated.
- Schema-driven: columns, validation rules, and totals are defined in a
  JSON schema file (`src/file_pipeline/schemas/sales.json`), so the same
  engine can process other kinds of CSV.
- Decimal-based revenue calculations using Python's `Decimal` type instead
  of binary floating point.
- A rejection-rate limit: a file with more than half its rows rejected
  fails as a whole instead of producing untrustworthy totals.
- Product names compared case-insensitively, and warnings (never
  rejections) for repeated rows and prices far from a product's usual
  price.
- Duplicate-upload detection: the same file content uploaded again, under
  any name, is recorded as a duplicate of the original job and never
  counted twice.
- Queryable history: valid rows from every completed job, partitioned by
  month, as an Athena table with saved example queries.
- A local CLI that produces the same output format as the AWS deployment,
  so behavior can be verified without AWS access.
- An AWS Lambda handler with idempotent, at-least-once job processing:
  deterministic job IDs, DynamoDB-backed claims with expiring leases, and
  safe retries. Jobs whose retries stop are `dead_lettered` and
  announced; `scripts/reprocess.py` re-runs any job, for example after a
  rule change. A `manifest.json` written last marks a job's outputs
  complete.
- An upload page (`tools/upload.html`) and 7-day signed download links
  in notification emails, served by a token-protected API behind API
  Gateway.
- Outcomes published as CloudWatch metrics (dashboard included) and as
  EventBridge events; alarms for errors, the dead-letter queue, and a
  stuck queue.
- Data at rest encrypted with a customer-managed KMS key.
- Terraform configuration for the full AWS stack (S3, SQS, Lambda,
  DynamoDB, SNS, EventBridge, API Gateway, KMS, CloudWatch, Glue,
  Athena), plus a separate bootstrap stack for Terraform state; separate
  dev/prod environments supported.
- Snapshot tests that pin every sample's exact output bytes,
  property-based tests (Hypothesis) that check invariants over thousands
  of generated files, and a smoke test for a deployed stack.
- GitHub Actions CI on pushes to `master` and every pull request: ruff,
  mypy, pytest with a 90% coverage floor, pip-audit, and a checkov scan
  of the Terraform; Dependabot for dependency updates. A separate,
  manually triggered workflow deploys.
- Design decisions recorded in [`docs/decisions/`](docs/decisions/).

## Architecture

![CSV sales processing architecture: an uploaded CSV moves from an S3 input bucket through an SQS queue to a Lambda processor, which claims the job and checks for duplicate content in DynamoDB, writes reports and curated rows to an S3 output bucket that Athena queries by month, and announces each outcome by SNS email and an EventBridge event, with a dead-letter queue, CloudWatch alarms and a dashboard covering failures.](docs/architecture.svg)

On the normal path, a Lambda invocation reads the exact object version
referenced by the triggering S3 event, validates and aggregates the CSV,
checks that its content is new, writes two report files and the curated
rows to the output bucket, then `manifest.json`, updates the DynamoDB job
record, and announces the outcome by SNS and EventBridge. Malformed input, duplicate content, a
duplicate delivery of an already-finished job, and internal errors each
take a different, shorter path:

![Job status lifecycle: a job is claimed into processing under a lease, then ends as completed, validation failed, or duplicate content; a retryable failure returns to processing on redelivery, while the fifth failed delivery or a code error dead-letters the job, which a redrive or the reprocess script sends back to processing.](docs/job-lifecycle.svg)

See [Reliability and limitations](#reliability-and-limitations) and
[decision record 0004](docs/decisions/0004-expiring-lease-claims.md) for
how claims and leases work.

Uploads from the browser and the download links in emails go through a
small portal API; files themselves move directly between the browser and
S3 on short-lived signed URLs:

![Portal: the upload page calls API Gateway with its token and uploads straight to the S3 input bucket with a presigned form; signed email links pass through API Gateway to the portal function, which reads job status from DynamoDB and redirects downloads to short-lived URLs on the output bucket.](docs/portal.svg)

## Local Quick Start

Prerequisites: Python 3.12+. Input files must be UTF-8 (an optional
leading byte-order mark is accepted and stripped); the default maximum
input size is 10 MiB, overridable with `--max-bytes`.

Run all commands below from the repository root.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

(`make install` does the same; `make help` lists the other everyday
commands.)

Run the processor on a sample file:

```bash
python -m file_pipeline.local --input samples/valid_sales.csv --output-dir local-output/
```

Exit codes:

| Code | Meaning |
|---|---|
| `0` | Job completed (with or without rejected rows) |
| `1` | The whole file was rejected (malformed CSV or over the size limit) -- no output is written |
| `2` | The file was structurally valid but failed validation (`validation_failed`): zero rows passed, or more than the schema's limit (50% for sales) were rejected. `summary.json` and `rejected_rows.csv` are still written, so the rejection reasons are visible, and the reason is printed to stderr |

Other sample files, each covering one validation scenario, are in
[`samples/`](samples/): `mixed_valid_invalid.csv`, `missing_columns.csv`,
`all_invalid.csv`, `high_rejection_rate.csv` (3 of 5 rows rejected),
`warnings_example.csv` (a repeated row and a price typo),
`valid_sales.csv.gz` (gzip input), and `malformed.csv` (a
duplicate-header file).

To process a different kind of CSV, pass `--schema` with a bundled schema
name or a path to a schema file (see [Schemas](#schemas)):

```bash
python -m file_pipeline.local --input inventory.csv --output-dir out/ --schema my_schema.json
```

A guided walkthrough (valid upload, then an all-invalid file and how to read
its rejection reasons) is in [`scripts/demo_local.sh`](scripts/demo_local.sh).

## Input Requirements and Example Output

Required columns of the default `sales` schema (matched
case-insensitively, surrounding whitespace trimmed):

| Column | Requirement |
|---|---|
| `date` | `YYYY-MM-DD`, a real calendar date |
| `product` | Non-empty after trimming whitespace |
| `quantity` | Positive integer |
| `unit_price` | Non-negative, finite decimal (no `NaN`/`Infinity`, no scientific notation) |

Example input:

```csv
date,product,quantity,unit_price
2024-01-05,Widget,3,9.99
2024-01-06,Gadget,1,19.50
2024-02-30,Widget,2,9.99
2024-01-06,,1,5.00
```

`summary.json`:

```json
{
  "by_product": {
    "Gadget": { "quantity": 1, "revenue": "19.50" },
    "Widget": { "quantity": 3, "revenue": "29.97" }
  },
  "rejected_row_count": 2,
  "total_revenue": "49.47",
  "valid_row_count": 2,
  "warnings": {
    "duplicate_rows": { "count": 0, "row_numbers": [] },
    "outliers": { "column": "unit_price", "count": 0, "row_numbers": [] }
  }
}
```

`warnings` never changes totals or status. `duplicate_rows` lists valid
rows equal to an earlier one (prices compared by value, product names
without case); `outliers` lists rows whose unit price is more than 10x
above or below that product's median in the file. Each gives an exact
count and up to 20 row numbers.

`rejected_rows.csv`:

```csv
date,product,quantity,unit_price,row_number,rejection_reason
2024-02-30,Widget,2,9.99,3,invalid_date
2024-01-06,,1,5.00,4,empty_product
```

A file missing a required column, with duplicate headers, that cannot be
decoded, or with a field exceeding Python's CSV field-size limit is
rejected as a whole file, with nothing written. Malformed quoting,
including an unterminated quoted field, is not a file-level error:
Python's CSV parser absorbs everything through the next matching quote,
or the end of the file, into a single field rather than raising a parse
error. This typically surfaces as a row-level `row_length_mismatch`
rejection for that row, and can absorb what looks like a subsequent data
row into the same field instead of parsing it separately. An individual
invalid row is otherwise recorded in `rejected_rows.csv` with a reason
and excluded from the totals, while the rest of the file is still
processed. If every row turns out invalid (`no_valid_rows`), or more than
half of them do (`rejection_rate_exceeded`; the limit is the schema's
`max_rejection_rate`, and exactly 50% still completes), the job status is
also `validation_failed`, but `summary.json` and `rejected_rows.csv` are
still produced, showing the counts and why each row failed. This
distinction matters when retrieving reports: a file-level rejection
produces no report to download, while a structurally valid file that
failed validation does.

Extra columns are allowed and ignored for valid rows. For a rejected row,
they are retained in `rejected_rows.csv`, which echoes the raw row,
subject to the same spreadsheet formula-injection escaping described
below and applied to every field.

Monetary values are computed with Python's `Decimal` type at its default
precision of 28 significant digits, not binary floating point, and
serialized as plain decimal strings with no scientific notation and no
float rounding error. This is exact arithmetic within that precision, not
arbitrary-precision arithmetic, and does not guarantee exactness for
inputs whose aggregated values exceed 28 significant digits.

Fields written to `rejected_rows.csv` that begin with `=`, `+`, `-`, `@`,
a tab, or a carriage return are prefixed with a leading apostrophe before
being written, per OWASP's CSV-injection guidance. This prevents a value
such as `=SUM(A1:A9)` in a rejected row from executing as a formula when
the file is opened in spreadsheet software.

<details>
<summary>Row-level rejection reason codes and internal field names</summary>

Row-level rejection reasons: `row_length_mismatch`, `invalid_date`,
`empty_product`, `invalid_quantity`, `non_positive_quantity`,
`invalid_unit_price`, `negative_unit_price`, `non_finite_unit_price`. A
duplicate header name (after normalization) makes the whole file
malformed, since the column a value belongs to would be ambiguous. A row
with a different number of fields than the header is rejected
individually, not treated as a file-level error.

In the AWS deployment, a file-level rejection and a structurally valid
file that failed validation are both recorded with the same
`validation_failed` status on the DynamoDB job record. `error_code` says
why (`missing_required_columns`, `duplicate_header`, `input_too_large`,
... for a file-level rejection; `no_valid_rows` or
`rejection_rate_exceeded` otherwise), and `output_summary_key` /
`output_rejected_key` are present only when a report was produced. The
local CLI distinguishes them directly, through its exit code (`1` vs
`2`, above).

Final job statuses: `completed`, `completed_with_rejections`,
`validation_failed`, and `duplicate_content`. Not final: `processing`,
`failed` (will be retried), and `dead_lettered` (retrying stopped; see
[Reliability and limitations](#reliability-and-limitations)).

</details>

## Schemas

What a valid row is, and what gets added up, is defined by a JSON schema
file. The default, [`sales.json`](src/file_pipeline/schemas/sales.json):

```json
{
  "name": "sales",
  "version": 1,
  "columns": [
    { "name": "date", "type": "date" },
    { "name": "product", "type": "string", "required": true, "case_insensitive": true },
    { "name": "quantity", "type": "integer", "positive": true },
    { "name": "unit_price", "type": "decimal", "non_negative": true }
  ],
  "aggregation": {
    "group_by": "product",
    "measures": [
      { "name": "quantity", "multiply": ["quantity"] },
      { "name": "revenue", "multiply": ["quantity", "unit_price"], "total": true }
    ]
  },
  "delimiters": [",", ";", "\t"],
  "max_rejection_rate": 0.5,
  "warnings": {
    "duplicate_rows": true,
    "outliers": { "column": "unit_price", "per": "product", "factor": 10 }
  },
  "curated": { "partition_by_month": "date" }
}
```

Column types are `date`, `string`, `integer`, and `decimal`, each with a
few options (`required`, `case_insensitive`, `positive`, `signed`,
`non_negative`); rejection codes are generated from the column name
(`invalid_<name>`, `empty_<name>`, `non_positive_<name>`, ...). Each
measure adds up, per value of `group_by`, the product of its listed
columns; `total: true` also reports a grand total (`total_revenue`).
`delimiters` lists accepted field separators, `warnings` turns on the
checks above, `curated` writes rows for Athena, and `version` numbers the
rules (every job records the version it ran with). The full format is
documented in [`schema.py`](src/file_pipeline/schema.py), and an invalid
schema is rejected with a specific error before any file is processed.

The CLI takes `--schema`; the Lambda uses the `schema_name` Terraform
variable (one schema per deployment).

## AWS Deployment and Configuration

Prerequisites: Terraform >= 1.10 (required for S3-native state locking --
see [`infra/versions.tf`](infra/versions.tf)) and an authenticated AWS CLI
session (see [Authentication](#authentication)).

Two separate Terraform roots:

| Root | Purpose |
|---|---|
| `infra/bootstrap` | Creates the S3 bucket that holds the main stack's Terraform state. Uses local state itself. |
| `infra` | The application stack: S3 buckets, SQS, Lambda, DynamoDB, SNS, CloudWatch, Glue, Athena. Uses an S3 backend. |

`aws_region` and `project_name` must be set to the same values in both
roots. `infra/bootstrap` names the state bucket and, when GitHub OIDC is
enabled, scopes the deploy role's permissions from its own copy of these
variables; a mismatch between the two roots leaves the state bucket or the
deploy role pointed at resources under a different name or region than the
application stack actually uses.

### Authentication

The AWS CLI, Terraform, and Boto3 all read credentials from the standard
AWS credential chain, which can include long-lived access keys if you
configure it that way. The recommended options here use temporary
credentials instead:

- From a laptop: an AWS CLI profile backed by IAM Identity Center SSO
  (`aws configure sso`) or the AWS CLI's browser-based `aws login`.
- From GitHub Actions: the deploy workflow (`deploy.yml`) is wired for
  OIDC only (`aws-actions/configure-aws-credentials` with
  `role-to-assume`) -- it has no path for static access keys.

### Deploying

```bash
# 1. One-time: create the Terraform state bucket
cd infra/bootstrap
terraform init
terraform plan -out=tfplan
# Review the plan above -- it should show only the state bucket and its
# supporting resources -- before applying it.
terraform apply tfplan
terraform output -raw backend_hcl > ../backend.hcl
cd ..

# 2. The application stack (now in infra/, using the backend file just written)
terraform init -backend-config=backend.hcl
terraform plan -out=tfplan
# Review this plan before applying it too.
terraform apply tfplan

# Return to the repository root -- later commands in this README assume it.
cd ..
```

Key variables (see [`infra/variables.tf`](infra/variables.tf) for the full
list with defaults):

| Variable | Purpose |
|---|---|
| `environment` | Environment tag on every resource (default `prod`); see [Environments](#environments). |
| `aws_region` | Region for every resource (default `us-east-2`). Some AWS accounts restrict which regions general workloads may run in via an AWS Organizations Service Control Policy; if resource creation fails with a region-specific authorization error, check for such a policy and set `aws_region` accordingly. |
| `notification_email` | Subscribes an email address to the SNS topic. Empty by default. |
| `lambda_max_concurrency` | Caps concurrent Lambda invocations from the SQS trigger (2-1000, default 5). |
| `schema_name` | Bundled schema the Lambda uses (default `sales`); also names the Athena table. |
| `enable_athena` | Creates the Glue table, Athena workgroup, and query-results bucket (default `true`, when the schema has a `curated` section). |
| `athena_bytes_scanned_cutoff` | Athena cancels any single query that would scan more than this (default 1 GiB). |
| `athena_month_range` | Months visible to Athena queries (default `2000-01,NOW`). |
| `enable_events` | Publish every job outcome to the default EventBridge bus (default `true`). |
| `enable_portal`, `report_link_days`, `portal_requests_per_second` | The upload page's API and the email download links (default on, links valid 7 days, 10 requests/s). |
| `enable_kms` | Encrypt data at rest with the project's customer-managed KMS key (default `true`). |
| `enable_budget_alert`, `budget_limit_usd`, `budget_alert_email` | Optional AWS Budget alert. |

Set these in a gitignored `infra/terraform.tfvars`. `infra/bootstrap` has
its own separate `variables.tf` and reads its own gitignored
`infra/bootstrap/terraform.tfvars` -- there is no sharing between the two
files, so if you override `aws_region` or `project_name` from their shared
defaults, set the same values in both files. Each tfvars file stays on
your machine and is never available to GitHub Actions -- a fresh checkout
in CI has no copy of it, so the deploy workflow only ever sees each root's
variable defaults plus the repository/environment variables listed below.
A local-only customization (for example a non-default `notification_email`)
needs to be reproduced another way for a GitHub Actions deploy: as an
explicit `-var` flag added to `deploy.yml`, or as Actions variables the
workflow is updated to pass through.

### Environments

`prod` is the default. A separate dev stack, with its own state file and
resource-name prefix, is set up from the templates in
[`infra/envs/`](infra/envs/README.md); `make init ENV=dev`,
`make plan ENV=dev`, and `make apply` then work on it, and `make plan`
refuses to run against another environment's state.

<details>
<summary>Enabling GitHub Actions deploys (optional)</summary>

1. In `infra/bootstrap`, set `enable_github_oidc = true` and
   `github_repository = "owner/repo"`, then apply. If an OIDC provider for
   `token.actions.githubusercontent.com` already exists in the account, set
   `existing_github_oidc_provider_arn` instead of creating a second one.
2. Create a GitHub Environment (`production` by default) with required
   reviewers and a deployment-branch rule. GitHub Actions jobs that declare
   an environment put that environment -- not the branch -- in the OIDC
   subject claim, and the generated trust policy matches on it exactly.
3. Set repository variables `AWS_DEPLOY_ROLE_ARN` (from the bootstrap
   output), `AWS_REGION`, and `TF_STATE_BUCKET`. These are Actions
   *variables*, not secrets -- none of them is sensitive.
4. Run the `Deploy infrastructure` workflow via `workflow_dispatch`. It
   supports two inputs: `plan` (default) and `apply`. Every run -- for
   either input -- first runs `terraform plan -out=tfplan`; the `apply`
   input additionally applies that freshly generated plan file in the same
   job. Pull requests never trigger this workflow.

</details>

### IAM Permissions

The processor Lambda's role is scoped to project resources and the
relevant prefixes on them: reading the input bucket, writing the output
bucket under `reports/` and `curated/`, consuming its own SQS queue,
reading and writing its own DynamoDB table, publishing to its own SNS
topic and to the default EventBridge bus, using the project KMS key, and
writing to its own CloudWatch log group and X-Ray -- see
[`infra/iam.tf`](infra/iam.tf). The portal's role may only create objects
under `uploads/` in the input bucket, read report files, and read job
records ([`infra/portal.tf`](infra/portal.tf)).

The optional GitHub Actions deploy role is defined in `infra/bootstrap`,
outside the stack it deploys, so it can never widen its own permissions.
The flip side: when the application stack starts using a new AWS
service, the role needs a matching grant in
[`infra/bootstrap/github_oidc.tf`](infra/bootstrap/github_oidc.tf),
applied from a laptop, before a GitHub deploy can create it.

## AWS Usage

Via the AWS console: upload a `.csv` object to the input bucket, then
download the report from `reports/{job_id}/` in the output bucket once
processing completes (see the note above on which outcomes produce a
report to download).

Via script. Replace each `<...>` placeholder with the actual value before
running -- a shell interprets literal angle brackets as redirection, so
these won't run as typed. Bucket, table, and queue names are available via
`terraform output` from `infra/` after deploying (for example `terraform
output -raw input_bucket_name`).

```bash
scripts/upload.sh <input-bucket-name> samples/mixed_valid_invalid.csv
# prints the bucket, key, version ID, and the resulting job_id

scripts/check_job.sh <job-table-name> <job-id>
# poll until status is completed / completed_with_rejections /
# validation_failed / duplicate_content (or dead_lettered)

scripts/fetch_report.sh <output-bucket-name> <job-id> ./downloaded-report/
# refuses until manifest.json exists, then verifies the files against it

scripts/list_jobs.sh <job-table-name> dead_lettered
# jobs with a given status, newest first
```

### Upload page and download links

Open [`tools/upload.html`](tools/upload.html) in a browser (it is a local
file; nothing is hosted). Paste the portal URL
(`terraform output -raw portal_url`) and the upload token
(`terraform output -raw upload_token`; the page never stores it), choose
a `.csv` or `.csv.gz`, and the page uploads it straight to S3, follows
the job, and shows its result with download buttons.

Notification emails for jobs with reports include download links valid
for 7 days. Each is signed for one job and file; the portal checks the
signature and redirects to a fresh 5-minute S3 link. Anyone holding the
email can use them until they expire. Design notes:
[decision record 0016](docs/decisions/0016-portal-and-signed-links.md).

### Re-running jobs

After a fix or a rule change (bump the schema's `version`), re-run jobs
with [`scripts/reprocess.py`](scripts/reprocess.py). It shows what it will
touch and asks first:

```bash
AWS_PROFILE=<profile> python scripts/reprocess.py --status dead_lettered
AWS_PROFILE=<profile> python scripts/reprocess.py --status completed --older-than-version 2 --dry-run
```

If the profile comes from `aws login`, boto3 needs the optional
`botocore[crt]` package to read it; otherwise export the session first
with `eval "$(aws configure export-credentials --profile <profile> --format env)"`
(`make smoke` does this for you).

A reprocessed job overwrites its own outputs, so nothing is counted
twice.

### Monitoring and events

The CloudWatch dashboard named after `project_name` shows job outcomes,
rows, Lambda health, queues, and Athena usage. Alarms (published to the
SNS topic) cover Lambda errors, ERROR log lines, messages in the
dead-letter queue, and a queue nobody is consuming.

Every outcome is also an EventBridge event on the default bus, so another
system can react to exactly what it needs, for example this rule pattern
for files that failed validation:

```json
{
  "source": ["csv-sales-pipeline"],
  "detail-type": ["CSV job finished"],
  "detail": { "status": ["validation_failed"] }
}
```

### Querying with Athena

Every valid row of every completed job is also written to
`curated/sales/month=YYYY-MM/{job_id}.json.gz` in the output bucket and
exposed as the table `sales` in the Glue database `csv_sales_pipeline`.
Each row has the CSV's columns, its `revenue`, and lineage: the
`job_id` it came from and its `source_row_number` in that file. Failed
and duplicate uploads never reach the table, so sums are never inflated.

In the Athena console, choose the `csv-sales-pipeline-analytics`
workgroup (`terraform output -raw athena_workgroup_name`); the queries in
[`docs/queries/sales/`](docs/queries/sales/) are saved there. For
example:

```sql
SELECT month, min(product) AS product, SUM(quantity) AS quantity, SUM(revenue) AS revenue
FROM sales
GROUP BY month, lower(product)
ORDER BY month, revenue DESC;
```

Curated rows keep each row's own spelling of the product name; grouping
by `lower(product)` and showing `min(product)` matches how the reports
group names.

New months are queryable as soon as their first file lands (partition
projection, no crawler). Filtering on `month` limits what Athena reads.
The workgroup cancels any query that would scan more than 1 GiB, and
query results are deleted after 7 days. Uploads processed before curated
output existed are not in the table; re-upload such a file once to add
it (it has no content fingerprint, so it is not treated as a duplicate).
Design notes: [decision record 0010](docs/decisions/0010-curated-json-lines-for-athena.md).

## Reliability and Limitations

- **Job identity.** The job ID is deterministic: `sha256(bucket + "\n" +
  key + "\n" + version_id)`. Re-delivering the same S3 event always
  produces the same job ID, which is what lets duplicate events be
  detected.
- **Duplicate delivery.** SQS and S3 event delivery are at-least-once. A
  duplicate event for an already-terminal job (`completed`,
  `completed_with_rejections`, `validation_failed`, or
  `duplicate_content`) does not reprocess the CSV; only
  `scripts/reprocess.py` does, on request. A failed or interrupted attempt is retried and can genuinely
  process the CSV again -- that is the intended recovery path.
- **Duplicate content.** A new upload of the same bytes (same or
  different key) is a new S3 version and therefore a new job, but its
  SHA-256 matches a job that already completed, so it ends as
  `duplicate_content` with `duplicate_of` naming the original, writes no
  reports or curated rows, and says so in its notification. If the
  original is still in progress, the duplicate is retried until the
  original finishes. Only files that produced totals are fingerprinted,
  and content is compared byte for byte -- see
  [decision record 0009](docs/decisions/0009-duplicate-content-fingerprints.md).
- **When retrying stops.** On the last allowed delivery a retryable
  failure marks the job `dead_lettered`, sends a notification, and lets
  the message move to the dead-letter queue; `scripts/redrive_dlq.sh`
  resumes it. An error raised by the processing code itself would recur
  on every attempt, so that job is dead-lettered at once and the
  notification names the reprocess command
  ([decision record 0013](docs/decisions/0013-dead-lettered-status.md)).
- **Lease-conditioned writes.** DynamoDB job-record updates after the
  initial claim require the matching lease token, so a worker whose lease
  has been taken over by a newer attempt cannot overwrite that attempt's
  result there. The S3 writes do not check the token, but they cannot
  race either: the Lambda timeout (60 s) is shorter than the lease
  (120 s), so a worker is stopped before its lease can be taken over.
- **Report writes.** `summary.json` and `rejected_rows.csv` are written
  through two separate `PutObject` calls, not one atomic operation, at
  fixed keys, so a retry corrects a partial write from an earlier failed
  attempt. Because the output bucket is versioned, each retry's writes add
  new object versions rather than replacing history. Curated files are
  written in the same step and are byte-for-byte deterministic, so a
  retry rewrites them identically. `manifest.json` is written last and
  lists every output with its SHA-256: until it exists, the job's
  outputs are incomplete.
- **Notifications and events.** Publishing to SNS and EventBridge and
  recording that success in DynamoDB are separate operations, so a
  subscriber can occasionally receive a duplicate notification or event
  for the same job. A successful SNS
  `Publish` call means SNS accepted the message for delivery to confirmed
  subscribers, not that a particular subscriber received it.

## Testing

```bash
make check      # ruff, mypy, all tests, terraform fmt + validate
make coverage   # tests with a coverage report (CI requires 90%)
make audit      # known vulnerabilities in installed dependencies
make security   # checkov scan of infra/ (pip install checkov)
```

Tests run without real AWS credentials or network calls: pure-Python logic
is tested directly, and the Lambda handler's AWS interactions are tested
against [moto](https://github.com/getmoto/moto)'s in-memory AWS emulation,
covering duplicate and concurrent delivery, lease expiry and recovery,
duplicate content, transient AWS errors, partial-output-write retry,
SNS-failure retry, S3 keys with spaces and escaped characters, and
retrieval of a specific object version.

Two kinds of test guard the processing logic as a whole:

- **Snapshot tests** (`tests/test_golden.py`) run every file in
  `samples/` through both the CLI and the Lambda and compare the output
  byte for byte with `tests/golden/`. After an intended output change,
  regenerate with `UPDATE_GOLDEN=1 pytest tests/test_golden.py` and
  review the diff.
- **Property-based tests** (`tests/test_properties.py`) use
  [Hypothesis](https://hypothesis.works/) to generate thousands of
  random sales files whose correct result is known row by row, and check
  that totals match an independent calculation, row order never changes
  the summary, every rejection is reported with its reason, the curated
  rows add up to the report, and CLI and Lambda output are identical.

`tests/test_terraform_schema.py` evaluates the Athena column list in
Terraform (offline, with `terraform console`) and checks that it matches
the Python side; it is skipped when Terraform is not installed.

checkov exceptions are deliberate and each has its reason, in
[`.checkov.yaml`](.checkov.yaml) or beside the resource.

For a deployed stack, `make smoke` ([`scripts/smoke_test.py`](scripts/smoke_test.py))
uploads a freshly generated file and checks that reports match the local
CLI byte for byte, that manifest hashes match, that Athena agrees with
the report, that a re-upload is caught as a duplicate, and that the
portal's upload, downloads, and signed links work. It leaves its files
behind as job history.

## Project Structure

```
src/file_pipeline/
  processor.py      # CSV validation and aggregation (no AWS dependency)
  schema.py         # Schema file format and validation
  schemas/          # Bundled schemas (sales.json)
  curated.py        # Curated rows for Athena
  local.py          # CLI runner
  storage.py        # S3 reads/writes, manifest
  jobs.py           # DynamoDB job state, claims, leases, content fingerprints
  notifications.py  # SNS messages, EventBridge events
  handler.py        # SQS-triggered Lambda entry point
  portal.py         # Upload/status/link API (behind API Gateway)
  links.py          # Signed report links
  admin.py          # Listing and reprocessing jobs (operator side)
tests/              # pytest; moto for AWS interaction tests
tests/golden/       # Snapshot outputs for every sample
samples/            # Example CSVs, one per validation scenario
tools/upload.html   # Local upload page
infra/              # Terraform (main stack)
infra/envs/         # Templates for extra environments (dev)
infra/bootstrap/    # Terraform (state bucket, optional OIDC role)
docs/decisions/     # Design decision records
docs/queries/       # Example Athena queries (saved in the workgroup)
docs/diagrams/      # Diagram sources (HTML); exported SVGs are in docs/
scripts/            # Upload, status, report, list, reprocess, redrive,
                    # smoke-test, and teardown helpers
Makefile            # Everyday commands (make help)
.github/            # CI (always), deploy (manual, gated), Dependabot
```

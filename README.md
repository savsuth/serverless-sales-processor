# Serverless Sales Processor

[![CI](https://github.com/savsuth/serverless-sales-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/savsuth/serverless-sales-processor/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Serverless Sales Processor turns a sales CSV into revenue and quantity
totals for each product. It checks every row, totals the valid ones, and
lists each invalid row with the reason it was rejected, so one bad line
never sinks the whole file. Money is calculated with Python's `Decimal`
type rather than floating point, so totals carry no rounding error.

The same processing code runs in two places: as a command-line tool on
your machine, and on AWS, where uploading a file to S3 starts the work.
On AWS, every upload becomes a job that is tracked from start to finish
and announced through SNS, usually by email. A retried or repeated event never processes a
finished job a second time, and a file whose content was already
processed is recognized as a duplicate and not counted again. Valid rows
are also saved as a table you can query with Amazon Athena. Files can be
uploaded from a small web page, and notification emails include download
links.

This repository contains the application, its tests, and the Terraform
configuration that deploys the AWS stack.

## Features

**Processing**

- **Streaming validation.** Files are read row by row, never loaded into
  memory whole. Accepts `.csv` and `.csv.gz` in any letter case,
  separated by commas, semicolons, or tabs. Required columns and
  duplicate headers are checked first, then every row.
- **Schema-driven rules.** Columns, validation rules, and totals are
  defined in a JSON schema file
  ([`sales.json`](src/file_pipeline/schemas/sales.json)), so the same
  engine can process other kinds of CSV.
- **Exact money.** Revenue is calculated with `Decimal`, not binary
  floating point.
- **Rejection limit.** If more than half of a file's rows are rejected,
  the whole file fails instead of producing totals you cannot trust.
- **Data-quality warnings.** Product names are compared without regard
  to case. Repeated rows, and prices far from a product's usual price,
  are flagged as warnings; they never cause a rejection.
- **Local CLI.** Produces exactly the same output as the AWS deployment,
  so you can check behavior without an AWS account.

**AWS pipeline**

- **Safe, repeatable jobs.** Each upload gets a deterministic job ID and
  is claimed in DynamoDB with an expiring lease, so retries are safe and
  a finished job is never processed twice by accident. A
  `manifest.json`, written last, marks a job's outputs as complete.
- **Duplicate detection.** The same content uploaded again, under any
  name, is recorded as a duplicate of the original job and never counted
  twice.
- **Failure handling.** A job whose retries run out ends as
  `dead_lettered`, and you are notified. `scripts/reprocess.py` re-runs
  any job, for example after a rule change.
- **Queryable history.** Valid rows from every completed job form an
  Athena table, partitioned by month, with saved example queries.
- **Upload page and download links.** `tools/upload.html` uploads files
  through a token-protected API behind API Gateway, and notification
  emails carry download links that stay valid for 7 days.
- **Monitoring.** Outcomes are published as CloudWatch metrics, with a
  dashboard, and as EventBridge events. Alarms cover errors, the
  dead-letter queue, and a stuck queue.
- **Encryption.** Data at rest is encrypted with a customer-managed KMS
  key.

**Infrastructure and quality**

- **Terraform** for the whole stack (S3, SQS, Lambda, DynamoDB, SNS,
  EventBridge, API Gateway, KMS, CloudWatch, Glue, and Athena), plus a
  separate bootstrap stack for Terraform state. Separate dev and prod
  environments are supported.
- **Tests.** Snapshot tests pin the exact output of every sample file,
  property-based tests (Hypothesis) check invariants across thousands of
  generated files, and a smoke test checks a deployed stack end to end.
- **CI.** GitHub Actions runs ruff, mypy, pytest (with a 90% coverage
  floor), pip-audit, and a checkov scan of the Terraform on every pull
  request and every push to `master`. Dependabot proposes dependency
  updates, and a separate, manually triggered workflow deploys.
- **Documented decisions.** Design choices are recorded in
  [`docs/decisions/`](docs/decisions/).

## Architecture

![CSV sales processing architecture: an uploaded CSV moves from an S3 input bucket through an SQS queue to a Lambda processor, which claims the job and checks for duplicate content in DynamoDB, writes reports and curated rows to an S3 output bucket that Athena queries by month, and announces each outcome by SNS email and an EventBridge event, with a dead-letter queue, CloudWatch alarms and a dashboard covering failures.](docs/architecture.svg)

For a normal file, the Lambda function:

1. reads the exact object version named in the S3 event;
2. validates the rows and calculates the totals;
3. checks that the content has not been processed before;
4. writes the two reports and the curated rows to the output bucket,
   followed by `manifest.json`;
5. updates the job record in DynamoDB; and
6. announces the outcome by SNS email and an EventBridge event.

Malformed input, duplicate content, a repeated delivery of a finished
job, and internal errors each take a shorter path, as the job lifecycle
shows:

![Job status lifecycle: a job is claimed into processing under a lease, then ends as completed, validation failed, or duplicate content; a retryable failure returns to processing on redelivery, while the fifth failed delivery or a code error dead-letters the job, which a redrive or the reprocess script sends back to processing.](docs/job-lifecycle.svg)

[Decision record 0004](docs/decisions/0004-expiring-lease-claims.md)
explains how claims and leases work, and the other
[decision records](docs/decisions/) cover the rest of the design.

Browser uploads and email download links go through a small portal API.
The files themselves travel directly between the browser and S3 over
short-lived signed URLs:

![Portal: the upload page calls API Gateway with its token and uploads straight to the S3 input bucket with a presigned form; signed email links pass through API Gateway to the portal function, which reads job status from DynamoDB and redirects downloads to short-lived URLs on the output bucket.](docs/portal.svg)

## Local quick start

You need Python 3.12 or later. Run every command from the repository
root.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

`make install` does the same, and `make help` lists the other everyday
commands.

Process a sample file:

```bash
python -m file_pipeline.local --input samples/valid_sales.csv --output-dir local-output/
```

Input must be UTF-8; a leading byte-order mark is accepted and removed.
Files can be up to 10 MiB (for `.csv.gz`, measured after decompression),
and `--max-bytes` changes the limit.

The exit code tells you what happened:

| Code | Meaning |
|---|---|
| `0` | The job completed, with or without rejected rows. |
| `1` | The whole file was rejected, for example for a missing column, a duplicated header, or being over the size limit. Nothing is written. |
| `2` | The file was readable but failed validation (`validation_failed`): no row passed, or more rows were rejected than the schema allows (50% for sales). `summary.json` and `rejected_rows.csv` are still written so you can see why, and the reason is printed to stderr. |

Each file in [`samples/`](samples/) covers one scenario:

| File | Scenario |
|---|---|
| `valid_sales.csv` | Every row is valid. |
| `mixed_valid_invalid.csv` | 9 valid rows and 8 rejected ones, with six different rejection reasons and a formula-like value that gets escaped. |
| `all_invalid.csv` | Every row is rejected, so the file fails validation. |
| `high_rejection_rate.csv` | 3 of 5 rows are rejected, over the 50% limit. |
| `warnings_example.csv` | Two repeated rows (one differs only in letter case and price format) and a price typo, all flagged as warnings. |
| `missing_columns.csv` | A required column is missing, so the whole file is rejected. |
| `malformed.csv` | A duplicated header, so the whole file is rejected. |
| `valid_sales.csv.gz` | Gzip input. |

To process a different kind of CSV, pass `--schema` with the name of a
bundled schema or the path to a schema file (see [Schemas](#schemas)):

```bash
python -m file_pipeline.local --input inventory.csv --output-dir out/ --schema my_schema.json
```

[`scripts/demo_local.sh`](scripts/demo_local.sh) walks through a valid
upload, then an all-invalid file and how to read its rejection reasons.

## Input requirements and example output

The default `sales` schema requires these columns. Names are matched
without regard to case, and surrounding whitespace is trimmed.

| Column | Requirement |
|---|---|
| `date` | `YYYY-MM-DD`, a real calendar date |
| `product` | Not empty after trimming whitespace |
| `quantity` | A positive integer |
| `unit_price` | A non-negative, finite decimal (no `NaN` or `Infinity`, no scientific notation) |

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

`rejected_rows.csv`:

```csv
date,product,quantity,unit_price,row_number,rejection_reason
2024-02-30,Widget,2,9.99,3,invalid_date
2024-01-06,,1,5.00,4,empty_product
```

Warnings never change the totals or the job's status:

- `duplicate_rows` lists valid rows identical to an earlier row, with
  prices compared by value and product names compared without regard to
  case.
- `outliers` lists rows whose unit price is more than 10 times above or
  below that product's median price in the file.

Each warning gives an exact count and up to 20 row numbers.

### What happens to a bad file

| Problem | Result | Reports |
|---|---|---|
| A required column is missing, a header is duplicated, the file cannot be decoded, or a field is longer than Python's CSV field-size limit. | The whole file is rejected. | None |
| Some rows are invalid. | Each invalid row is listed in `rejected_rows.csv` with its reason and left out of the totals. The rest of the file is processed. | Yes |
| Every row is invalid (`no_valid_rows`), or more rows are rejected than the schema's `max_rejection_rate` allows (`rejection_rate_exceeded`; exactly 50% still completes). | The job fails as `validation_failed`. | Yes, showing the counts and why each row failed |

This difference matters when you fetch reports: a rejected file has
nothing to download, while a file that failed validation does.

Malformed quoting is not a file-level error. When a quote is never
closed, Python's CSV parser does not raise an error; it absorbs
everything up to the next matching quote, or the end of the file, into
a single field. This usually appears as a `row_length_mismatch`
rejection, and it can swallow what looks like the next data row into
the same field.

Extra columns are allowed. They are ignored in valid rows and kept in
`rejected_rows.csv`, which repeats each rejected row as it arrived.

Any field written to `rejected_rows.csv` that starts with `=`, `+`, `-`,
`@`, a tab, or a carriage return gets a leading apostrophe, following
OWASP's guidance on CSV injection. A value such as `=SUM(A1:A9)` in a
rejected row therefore cannot run as a formula when the file is opened
in a spreadsheet.

Money is calculated with `Decimal` at its default precision of 28
significant digits and written as plain decimal strings, with no
scientific notation and no floating-point rounding error. The arithmetic
is exact within that precision, but it is not arbitrary-precision:
totals that need more than 28 significant digits are not guaranteed to
be exact.

<details>
<summary>Rejection reason codes, job record fields, and job statuses</summary>

Row-level rejection reasons: `row_length_mismatch`, `invalid_date`,
`empty_product`, `invalid_quantity`, `non_positive_quantity`,
`invalid_unit_price`, `negative_unit_price`, and
`non_finite_unit_price`. A row with a different number of fields from
the header is rejected on its own; it does not make the file invalid. A
duplicate header name (after normalization) does, because it would be
unclear which column a value belongs to.

On AWS, a rejected file and a file that failed validation are both
recorded with the status `validation_failed` on the DynamoDB job record.
`error_code` says why: `missing_required_columns`, `duplicate_header`,
`input_too_large`, and so on for a rejected file, or `no_valid_rows` or
`rejection_rate_exceeded` otherwise. `output_summary_key` and
`output_rejected_key` are present only when reports were written. The
local CLI tells the two cases apart through its exit code (`1` or `2`,
above).

Final job statuses are `completed`, `completed_with_rejections`,
`validation_failed`, and `duplicate_content`. A job can also be
`processing`, `failed` (it will be retried), or `dead_lettered`
(retrying stopped; see
[decision record 0013](docs/decisions/0013-dead-lettered-status.md)).

</details>

## Schemas

A JSON schema file defines what a valid row is and what gets added up.
This is the default,
[`sales.json`](src/file_pipeline/schemas/sales.json):

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

- **Columns** have one of four types: `date`, `string`, `integer`, or
  `decimal`. The options are `required`, `case_insensitive`, `positive`,
  `signed`, and `non_negative`. Rejection codes are generated from the
  column name (`invalid_<name>`, `empty_<name>`, `non_positive_<name>`,
  and so on).
- **Measures** each add up the product of their listed columns for every
  value of `group_by`. `"total": true` also reports a grand total, such
  as `total_revenue`.
- **`delimiters`** lists the accepted field separators, **`warnings`**
  turns on the checks described above, and **`curated`** writes rows for
  Athena.
- **`version`** numbers the rules. Every job records the version it ran
  with.

[`schema.py`](src/file_pipeline/schema.py) documents the full format. An
invalid schema is rejected with a specific error before any file is
processed. The CLI takes `--schema`; on AWS, the `schema_name` Terraform
variable selects the schema, one per deployment.

## AWS deployment and configuration

You need Terraform 1.10 or later, for S3-native state locking (see
[`infra/versions.tf`](infra/versions.tf)), and an authenticated AWS CLI
session (see [Authentication](#authentication)).

The Terraform is split into two roots:

| Root | Purpose |
|---|---|
| `infra/bootstrap` | Creates the S3 bucket that holds the main stack's Terraform state. It keeps its own state locally. |
| `infra` | The application stack: S3 buckets, SQS, Lambda, DynamoDB, SNS, EventBridge, API Gateway, KMS, CloudWatch, Glue, and Athena. Its state lives in that bucket. |

Set `aws_region` and `project_name` to the same values in both roots.
The bootstrap root uses its own copy of them to name the state bucket
and, when GitHub OIDC is enabled, to scope the deploy role's
permissions. If the two roots disagree, the state bucket or the deploy
role ends up pointing at a different name or region from the
application stack.

### Authentication

The AWS CLI, Terraform, and boto3 all read credentials from the standard
AWS credential chain. That chain can hold long-lived access keys, but
the options recommended here use temporary credentials instead:

- **From a laptop:** an AWS CLI profile backed by IAM Identity Center
  (`aws configure sso`), or the browser-based `aws login`.
- **From GitHub Actions:** the deploy workflow (`deploy.yml`) supports
  OIDC only, through `aws-actions/configure-aws-credentials` with
  `role-to-assume`. It has no way to use static access keys.

### Deploying

```bash
# 1. One time only: create the Terraform state bucket
cd infra/bootstrap
terraform init
terraform plan -out=tfplan
# Review the plan. It should show only the state bucket and its
# supporting resources.
terraform apply tfplan
terraform output -raw backend_hcl > ../backend.hcl
cd ..

# 2. The application stack, in infra/, using the backend file just written
terraform init -backend-config=backend.hcl
terraform plan -out=tfplan
# Review this plan too before applying it.
terraform apply tfplan

# Return to the repository root; the rest of this README assumes it.
cd ..
```

The most important variables are below. [`infra/variables.tf`](infra/variables.tf)
lists all of them with their defaults.

| Variable | Purpose |
|---|---|
| `environment` | The Environment tag on every resource (default `prod`). See [Environments](#environments). |
| `aws_region` | The region for every resource (default `us-east-2`). Some AWS accounts use an AWS Organizations service control policy to limit which regions workloads may run in. If resource creation fails with a region-specific authorization error, check for such a policy and set `aws_region` to match. |
| `notification_email` | An email address to subscribe to the SNS topic. Empty by default. |
| `lambda_max_concurrency` | The most Lambda invocations the SQS trigger runs at once (2 to 1000, default 5). |
| `lambda_memory_mb` | The processor's memory, which also sets its share of CPU (default 1024). It is sized so the largest allowed file finishes well within the 60-second timeout; see [docs/costs.md](docs/costs.md#timeout-headroom). |
| `schema_name` | The bundled schema the Lambda uses (default `sales`). It also names the Athena table. |
| `enable_athena` | Creates the Glue table, the Athena workgroup, and the query-results bucket (default `true`; takes effect when the schema has a `curated` section). |
| `athena_bytes_scanned_cutoff` | Athena cancels any query that would scan more than this (default 1 GiB). |
| `athena_month_range` | The months Athena queries can see (default `2000-01,NOW`). |
| `enable_events` | Publishes every job outcome to the default EventBridge bus (default `true`). |
| `enable_portal`, `report_link_days`, `portal_requests_per_second` | The upload page's API and the email download links (on by default; links stay valid for 7 days; 10 requests per second). |
| `enable_kms` | Encrypts data at rest with the project's customer-managed KMS key (default `true`). |
| `upload_retention_days` | How many days to keep uploaded CSVs (default `0`, which keeps them forever; otherwise at least 30). Reports and curated data never expire, and an expired upload can no longer be reprocessed. |
| `enable_budget_alert`, `budget_limit_usd`, `budget_alert_email` | An optional AWS Budgets alert. |

Put your settings in `infra/terraform.tfvars`, which git ignores. The
bootstrap root has its own `variables.tf` and its own ignored
`infra/bootstrap/terraform.tfvars`, and nothing is shared between them:
if you change `aws_region` or `project_name`, change them in both files.

These files never leave your machine, so GitHub Actions never sees them.
A deploy from Actions uses each root's defaults plus the repository
variables described below. To carry a local setting (for example
`notification_email`) into an Actions deploy, add it to `deploy.yml` as
a `-var` flag, or create an Actions variable and update the workflow to
pass it through.

### Environments

`prod` is the default. A separate dev stack, with its own state file and
its own resource-name prefix, is set up from the templates in
[`infra/envs/`](infra/envs/README.md). `make init ENV=dev`,
`make plan ENV=dev`, and `make apply` then work on it, and `make plan`
refuses to run against another environment's state.

<details>
<summary>Enabling GitHub Actions deploys (optional)</summary>

1. In `infra/bootstrap`, set `enable_github_oidc = true` and
   `github_repository = "owner/repo"`, then apply. If the account already
   has an OIDC provider for `token.actions.githubusercontent.com`, set
   `existing_github_oidc_provider_arn` instead of creating a second one.
2. Create a GitHub Environment (named `production` by default) with
   required reviewers and a deployment-branch rule. A job that declares
   an environment puts that environment, not the branch, in its OIDC
   subject claim, and the generated trust policy matches it exactly.
3. Set the repository variables `AWS_DEPLOY_ROLE_ARN` (from the
   bootstrap output), `AWS_REGION`, and `TF_STATE_BUCKET`. They are
   Actions variables, not secrets, because none of them is sensitive.
4. Run the `Deploy infrastructure` workflow with `workflow_dispatch`. It
   takes one input, `plan` (the default) or `apply`. Every run starts
   with `terraform plan -out=tfplan`; with `apply`, the same job then
   applies that plan. Pull requests never trigger this workflow.

</details>

### IAM permissions

The processor Lambda's role ([`infra/iam.tf`](infra/iam.tf)) is limited
to this project's resources and the relevant prefixes on them. It can:

- read the input bucket, and write to the output bucket under
  `reports/` and `curated/`;
- consume its own SQS queue, and read and write its own DynamoDB table;
- publish to its own SNS topic and to the default EventBridge bus;
- use the project's KMS key; and
- write to its own CloudWatch log group and to X-Ray.

The portal's role ([`infra/portal.tf`](infra/portal.tf)) can only create
objects under `uploads/` in the input bucket, read report files, and
read job records.

The optional GitHub Actions deploy role is defined in `infra/bootstrap`,
outside the stack it deploys, so it can never widen its own permissions.
The trade-off: when the application stack starts using a new AWS
service, the role needs a matching grant in
[`infra/bootstrap/github_oidc.tf`](infra/bootstrap/github_oidc.tf),
applied from a laptop, before a GitHub deploy can create it.

## Using the deployed stack

In the AWS console, upload a `.csv` or `.csv.gz` file to the input
bucket. When the job finishes, download its report from
`reports/{job_id}/` in the output bucket. Not every outcome produces a
report; see [What happens to a bad file](#what-happens-to-a-bad-file).

The scripts below do the same from a terminal. Replace each `<...>`
placeholder first: the shell reads angle brackets as redirection, so
the commands fail as typed. After deploying, `terraform output` in
`infra/` prints the names you need, for example
`terraform output -raw input_bucket_name`.

```bash
scripts/upload.sh <input-bucket-name> samples/mixed_valid_invalid.csv
# prints the bucket, key, version ID, and the resulting job_id

scripts/check_job.sh <job-table-name> <job-id>
# poll until the status is completed, completed_with_rejections,
# validation_failed, or duplicate_content (or dead_lettered)

scripts/fetch_report.sh <output-bucket-name> <job-id> ./downloaded-report/
# refuses until manifest.json exists, then checks the files against it

scripts/list_jobs.sh <job-table-name> dead_lettered
# jobs with a given status, newest first
```

### Upload page and download links

Open [`tools/upload.html`](tools/upload.html) in a browser. It is a
local file; nothing is hosted. Paste in the portal URL
(`terraform output -raw portal_url`) and the upload token
(`terraform output -raw upload_token`; the page never stores it), then
choose a `.csv` or `.csv.gz` file. The page uploads it straight to S3,
follows the job, and shows the result with download buttons.

When a job has reports, its notification email includes download links
that stay valid for 7 days. Each link is signed for one job and one
file. The portal checks the signature and redirects to a fresh S3 link
that lasts 5 minutes. Anyone who has the email can use the links until
they expire. Design notes are in
[decision record 0016](docs/decisions/0016-portal-and-signed-links.md).

### Re-running jobs

After a fix or a rule change (bump the schema's `version`), re-run jobs
with [`scripts/reprocess.py`](scripts/reprocess.py). It shows which jobs
it will touch and asks before changing anything:

```bash
AWS_PROFILE=<profile> python scripts/reprocess.py --status dead_lettered
AWS_PROFILE=<profile> python scripts/reprocess.py --status completed --older-than-version 2 --dry-run
```

If your profile comes from `aws login`, boto3 can read it only with the
optional `botocore[crt]` package. Without that package, export the
session and its region first, then run the script without
`AWS_PROFILE`:

```bash
eval "$(aws configure export-credentials --profile <profile> --format env)"
export AWS_DEFAULT_REGION=<region>
python scripts/reprocess.py --status dead_lettered
```

`make smoke` does this for you.

A reprocessed job overwrites its own outputs, so nothing is counted
twice. If `upload_retention_days` is set, jobs whose upload has expired
are listed as skipped instead of being queued.

When a job ends as `dead_lettered` because AWS kept failing, rather than
because of the file, its message waits in the dead-letter queue for 14
days. Once you have fixed the cause, send every waiting message back for
another attempt. Both values come from `terraform output`
(`dead_letter_queue_url` and `processing_queue_arn`):

```bash
scripts/redrive_dlq.sh <dead-letter-queue-url> <processing-queue-arn>
```

### Monitoring and events

The CloudWatch dashboard named after `project_name` shows job outcomes,
row counts, Lambda health, the queues, and Athena usage. Alarms, sent to
the SNS topic, cover Lambda errors, ERROR log lines, messages in the
dead-letter queue, and a queue that nothing is consuming.

Every outcome is also an event on the default EventBridge bus, so
another system can react to exactly what it needs. For example, this
rule pattern matches files that failed validation:

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
Each row has the CSV's columns, its `revenue`, and its lineage: the
`job_id` it came from and its `source_row_number` in that file. Failed
and duplicate uploads never reach the table, so sums are never inflated.

In the Athena console, choose the `csv-sales-pipeline-analytics`
workgroup (`terraform output -raw athena_workgroup_name`). The queries
in [`docs/queries/sales/`](docs/queries/sales/) are saved there, for
example:

```sql
SELECT month, min(product) AS product, SUM(quantity) AS quantity, SUM(revenue) AS revenue
FROM sales
GROUP BY month, lower(product)
ORDER BY month, revenue DESC;
```

Curated rows keep each row's own spelling of the product name. Grouping
by `lower(product)` and showing `min(product)` matches how the reports
group names.

A new month becomes queryable as soon as its first file lands (through
partition projection, with no crawler), and filtering on `month` limits
what Athena reads. The workgroup cancels any query that would scan more
than 1 GiB, and query results are deleted after 7 days.

Uploads processed before curated output existed are not in the table.
To add one, upload the file again once; it has no content fingerprint,
so it is not treated as a duplicate. Design notes are in
[decision record 0010](docs/decisions/0010-curated-json-lines-for-athena.md).

## Costs

Measured on the deployed stack, processing costs about 0.09 USD per
1,000 small files and 0.38 USD per 1,000 files at the 10 MiB limit. The
KMS key adds about 1 USD a month once free tiers apply.
[docs/costs.md](docs/costs.md) covers hourly metric charges, storage
growth, and how the numbers were measured.

## Testing

```bash
make check      # ruff, mypy, all tests, terraform fmt + validate
make coverage   # tests with a coverage report (CI requires 90%)
make audit      # known vulnerabilities in installed dependencies
make security   # checkov scan of infra/ (pip install checkov)
```

The tests need no AWS credentials and no network. Pure-Python logic is
tested directly. The Lambda handler's AWS calls run against
[moto](https://github.com/getmoto/moto), an in-memory AWS emulator, and
cover:

- duplicate and concurrent delivery;
- lease expiry and recovery;
- duplicate content;
- transient AWS errors;
- retries after a partial output write or a failed SNS publish;
- S3 keys with spaces and escaped characters; and
- reading a specific object version.

Two kinds of test guard the processing logic as a whole:

- **Snapshot tests** (`tests/test_golden.py`) run every file in
  `samples/` through both the CLI and the Lambda and compare the output,
  byte for byte, with `tests/golden/`. After an intended change to the
  output, regenerate them with `UPDATE_GOLDEN=1 pytest tests/test_golden.py`
  and review the diff.
- **Property-based tests** (`tests/test_properties.py`) use
  [Hypothesis](https://hypothesis.works/) to generate thousands of random
  sales files whose correct result is known row by row. They check that:
  - totals match an independent calculation;
  - row order never changes the summary;
  - every rejection is reported with its reason;
  - the curated rows add up to the report; and
  - the CLI and the Lambda produce identical output.

`tests/test_terraform_schema.py` evaluates the Athena column list in
Terraform (offline, with `terraform console`) and checks that it matches
the Python side. It is skipped when Terraform is not installed.

Every checkov exception is deliberate and comes with its reason, either
in [`.checkov.yaml`](.checkov.yaml) or next to the resource.

For a deployed stack, `AWS_PROFILE=<profile> make smoke`
([`scripts/smoke_test.py`](scripts/smoke_test.py)) uploads a freshly
generated file and checks that:

- the reports match the local CLI's output byte for byte;
- the hashes in `manifest.json` match the files;
- Athena agrees with the report;
- uploading the same file again is caught as a duplicate; and
- the portal's upload, downloads, and signed links work.

It leaves its files behind as job history.

## Project structure

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
docs/costs.md       # Measured cost per file and per month
docs/queries/       # Example Athena queries (saved in the workgroup)
docs/diagrams/      # Diagram sources (HTML); exported SVGs are in docs/
scripts/            # Upload, status, report, list, reprocess, redrive,
                    # smoke-test, and teardown helpers
Makefile            # Everyday commands (make help)
LICENSE             # MIT
.github/            # CI (always), deploy (manual, gated), Dependabot
```

## License

Released under the [MIT License](LICENSE).

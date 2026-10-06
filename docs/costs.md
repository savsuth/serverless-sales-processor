# What the pipeline costs

Measured on 2026-10-06 against the deployed stack in us-east-2 (Lambda on
arm64, processor at 512 MB), priced with the AWS public price list for
US East (Ohio) as published in September and October 2026. All amounts
are list prices in USD **before** free tiers. The always-free tiers
(below) cover most of a small deployment.

## Summary

- **Each file costs well under a cent:** about 0.09 USD per 1,000
  small files and 0.38 USD per 1,000 files at the 10 MiB limit.
- **The fixed costs are larger than the per-file costs.** Running the
  stack with no uploads costs about 4.66 USD a month at list price, or
  1.00 USD (the KMS key) once free tiers apply.
- **At low volume, metrics outweigh processing.** The job metrics cost
  about 0.002 USD for each hour that has at least one upload. That is
  more than processing a single 10 MiB file.
- **Size the Lambda by its CPU, not its memory.** At 512 MB, a 10 MiB
  file of the shortest valid rows would need about 67 s and miss the
  60 s timeout. See [Timeout headroom](#timeout-headroom).

## Per file

Three measured files, each processed once and ending `completed`:

| | Small | 1 MB | 10 MB |
|---|---|---|---|
| Rows | 3 to 17 | 19,231 | 303,030 |
| Months of data (one curated file each) | 1 | 12 | 12 |
| Lambda billed duration, warm | 0.29 to 0.78 s | 3.37 s | 35.97 s |
| Lambda peak memory | 101 to 111 MB | 116 MB | 212 MB |
| **Cost per file** | **0.000087** | **0.000161** | **0.000378** |
| Cost per 1,000 files | 0.09 | 0.16 | 0.38 |
| Storage added, per month kept | under 0.000001 | 0.000028 | 0.00032 |

Where the money goes for each file:

| Item | Usage per file | Small | 1 MB | 10 MB |
|---|---|---|---|---|
| Lambda compute and request | billed s x 0.5 GB | 0.0000038 | 0.0000226 | 0.0002400 |
| S3 requests | 1 upload, 2 reads, 3 report writes, 1 write per month of data | 0.0000258 | 0.0000808 | 0.0000808 |
| KMS | up to 8 calls (see below) | 0.0000240 | 0.0000240 | 0.0000240 |
| SNS | 1 publish, 1 email | 0.0000205 | 0.0000205 | 0.0000205 |
| DynamoDB | 8 write units, 2 read units | 0.0000053 | 0.0000053 | 0.0000053 |
| X-Ray | 1 trace | 0.0000050 | 0.0000050 | 0.0000050 |
| SQS | 3 requests | 0.0000012 | 0.0000012 | 0.0000012 |
| EventBridge | 1 event | 0.0000010 | 0.0000010 | 0.0000010 |
| CloudWatch Logs | about 1.1 KB | 0.0000006 | 0.0000006 | 0.0000006 |

Notes:

- **Small files.** For a small file, S3 requests, KMS and the email
  together make up about 80% of the cost. Lambda is under 5%.
- **Large files.** Lambda dominates, and its time grows with the row
  count, not the byte count: about 0.12 ms per row at 512 MB.
- **Curated files.** Rows dated across many months mean more curated
  files, so more S3 writes: 12 months cost 0.000055 more than 1.
- **Duplicates.** A `duplicate_content` upload skips every report write
  and costs less than a small file.
- **Failed validation.** A file that fails validation writes no reports
  and costs less than a small file.
- **Retries.** Each retry repeats an attempt's Lambda time and most of
  its requests.
- **Storage.** Every upload is kept forever. Both buckets are versioned
  and nothing expires (`infra/s3.tf`), so storage grows by about 1.3 to
  1.5 times the input size per file and is billed every month. For
  example, 10,000 files of 1 MB come to about 13 GB, or 0.30 USD a month.
- **KMS.** "Up to 8" is what four back-to-back uploads used: 33 calls,
  counted in CloudTrail. S3 bucket keys, and the five-minute data-key
  reuse that SQS, DynamoDB and SNS apply, mean steady traffic needs
  fewer calls per file.

## Per hour with uploads: job metrics

Each finished attempt writes one CloudWatch Embedded Metric Format line
with four metrics (`JobAttempts`, `ValidRows`, `RejectedRows`,
`ProcessingMs`) under a `Status` dimension. CloudWatch charges custom
metrics by the hour, and only for hours in which data arrives.

- **The error metric adds one more.** The error-log metric filter's
  default value of 0 counts as data, so it adds one metric in any hour
  with activity.
- **The usual case is 5 metric-hours.** An hour in which every upload
  ends with the same status costs 5 metric-hours, about 0.002 USD,
  whether it had 1 upload or 10,000.
- **The ceiling is 25 metrics.** Six statuses can occur, giving
  6 x 4 + 1 = 25 metrics. Even with every status in every hour, the
  charge is capped at 7.50 USD a month.

At one upload an hour, metrics add about 1.50 USD a month, roughly 20
times the processing cost of small files. At thousands of uploads an hour, their cost
per file becomes negligible.

## Fixed monthly costs (no uploads)

| Item | List price | After always-free tiers |
|---|---|---|
| KMS customer-managed key | 1.00 | 1.00 |
| CloudWatch dashboard (1) | 3.00 | 0 (3 dashboards free) |
| CloudWatch alarms (4) | 0.40 | 0 (10 alarms free) |
| SQS polling by the Lambda trigger: 900 empty receives an hour, measured | 0.26 | 0 (1 million requests free) |
| **Total** | **4.66** | **1.00** |

KMS rotation: the key rotates yearly. The first and second rotations
each add 1 USD a month, so the key costs 1, then 2, then at most 3 USD a
month.

Not included:

- **Usage-based items with no charge while idle:** API Gateway, the
  portal Lambda and DynamoDB storage. DynamoDB storage is free up to
  25 GB.
- **Athena:** 5 USD per TB scanned, with a 10 MB minimum per query, so
  at least 0.00005 USD a query. The workgroup cancels any query over
  1 GiB.
- **Log storage** (0.03 USD per GB-month, 30-day retention) and the
  Terraform state bucket: both are fractions of a cent.

Free tiers that apply here: Lambda includes 400,000 GB-seconds and
1 million requests a month, which is about 22,000 files of 10 MB, or
far more small files. SNS includes 1,000 emails a month,
KMS 20,000 requests a month, and X-Ray 100,000 traces a month.

## Portal

An upload through `tools/upload.html` adds about six portal requests:
the upload form, the job lookup, status polls, and download links.

- **API Gateway:** 0.000001 USD a request.
- **Portal Lambda (256 MB):** warm requests take 0.06 to 0.28 s, about
  0.000001 USD each. A cold start takes about 2.7 s.

The portal adds roughly 0.00001 USD per upload.

## Timeout headroom

The processor's timeout is 60 s. A file can be up to 10 MiB after
decompression (the limit counts decompressed bytes). Processing time
follows the row count, and the shortest valid row is about 17 bytes
(`2024-03-28,B,5,2`), so the worst case is about 617,000 rows.

| File | Local, under moto | Lambda, 512 MB |
|---|---|---|
| 10 MB, 303,030 rows | 3.37 s | 35.97 s (measured) |
| 10 MiB, 616,807 rows | 6.31 s | about 67 s (scaled) |

At 512 MB the worst case would time out on every attempt and end
`dead_lettered`, although the file is within the documented limit.

Lambda gives CPU in proportion to memory: one full vCPU at 1,769 MB.
The default is therefore 1024 MB (`infra/variables.tf`). That roughly
halves the time, to about 34 s for the worst case. Peak memory for a
10 MB file was 212 MB, so the extra memory buys CPU, not space.

The cost per file barely moves. CPU-bound work at twice the memory
takes about half the time, so the GB-seconds stay about the same. Only
the fixed part of each run, about 0.3 s of network round trips, costs
twice as much: under 0.000002 USD more per file.

## How the numbers were measured

- **Lambda durations and memory.** From the `REPORT` lines in the
  processor's log group, read with CloudWatch Logs Insights:
  `filter @type = "REPORT" | fields @duration, @billedDuration,
  @maxMemoryUsed, @initDuration`. Since August 2025, billed duration
  includes the init phase of a cold start; the cold 100 KB run
  (1,961 rows) billed 2.76 s.
- **Test files.** The 1 MB and 10 MB files were synthetic and uploaded
  under `smoke-test/cost/`. Their rows are dated 1999, so Athena's
  partition projection (`2000-01,NOW`) never shows them in queries.
- **AWS calls per file.** The deployed handler was run under moto with
  a botocore `before-call` hook counting every request. Each outcome
  makes a fixed number of calls:

  | Outcome | Calls |
  |---|---|
  | `completed` | 14 |
  | `duplicate_content` | 12 |
  | `validation_failed` | 9 |
  | ignored non-CSV key | 0 |

  A `completed` file's 14 calls are: S3 HeadObject, GetObject and 4
  PutObject; DynamoDB 2 GetItem, 2 PutItem and 2 UpdateItem; 1 SNS
  Publish; 1 EventBridge PutEvents. A 12-month file makes 15 PutObject
  calls instead of 4.
- **DynamoDB units.** From the table's `ConsumedWriteCapacityUnits`
  metrics. A smoke run of three jobs used 12 table units and 12 index
  units (`status-created_at-index`), so 8 write units per file. The
  index costs as much as the table, because each status change
  rewrites the index entry.
- **KMS calls.** From CloudTrail event history (`kms.amazonaws.com`)
  for the minutes of the measurement uploads.
- **Idle SQS polling.** From the queue's `NumberOfEmptyReceives`: 900
  an hour, three hours in a row, with no uploads.
- **Log bytes.** From Logs Insights `sum(strlen(@message))` per
  invocation: 17,117 bytes over 16 processor runs.
- **Prices.** From `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<service>/current/us-east-2/index.json`,
  plus the CloudWatch and KMS pricing pages for dashboards, hourly
  metric proration and key rotation.

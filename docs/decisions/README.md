# Decision records

Short records of design decisions: the problem, what was chosen, and
what that costs. Records 0001-0006 write down decisions made in the
original build (previously explained only in code and Terraform
comments); 0007-0010 come with the schema, duplicate-detection and
Athena work.

| # | Decision |
|---|---|
| [0001](0001-deterministic-job-ids.md) | Job IDs are derived from the S3 object version, not generated |
| [0002](0002-sqs-between-s3-and-lambda.md) | An SQS queue sits between S3 and Lambda; concurrency is capped on the event source mapping |
| [0003](0003-timeout-lease-visibility-ordering.md) | Lambda timeout < lease < queue visibility timeout |
| [0004](0004-expiring-lease-claims.md) | Jobs are claimed with expiring leases, and every later write must hold the lease |
| [0005](0005-decimal-money.md) | Money is computed with Decimal and written as plain decimal strings |
| [0006](0006-two-terraform-roots.md) | Terraform is split into a bootstrap root and an application root |
| [0007](0007-json-schema-files.md) | Validation rules live in a JSON schema file |
| [0008](0008-rejection-threshold-in-schema.md) | The rejection-rate limit lives in the schema |
| [0009](0009-duplicate-content-fingerprints.md) | Duplicate uploads are recognized by a fingerprint of their content |
| [0010](0010-curated-json-lines-for-athena.md) | Curated data is gzip JSON Lines, partitioned by month with partition projection |

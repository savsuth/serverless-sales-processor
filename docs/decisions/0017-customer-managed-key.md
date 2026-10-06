# 0017. Data at rest uses one customer-managed KMS key

Status: accepted (2026-10-05)

## Context

Everything was encrypted with AWS-managed keys, which cannot be audited
per use, rotated on your schedule, or revoked.

## Decision

One key (`infra/kms.tf`, rotated yearly) encrypts the input, output and
Athena-results buckets (with S3 bucket keys), both queues, the job table,
the SNS topic, every log group, both Lambdas' environment variables, and
Athena results. Administration is delegated to IAM in the account.
Three services act for the stack and have their own key-policy
statements, because without them the failure is silent:

- S3, sending upload events to the encrypted queue (scoped to the input
  bucket and the account);
- CloudWatch, with alarms publishing to the encrypted topic;
- CloudWatch Logs, scoped to the account's log groups.

The Lambda roles may only Decrypt and GenerateDataKey on this key.

## Consequences

- About 1 USD a month plus requests (bucket keys keep S3 requests low).
- Objects written before the switch stay SSE-S3 and remain readable.
- The Terraform state bucket stays on SSE-S3, so state never depends on
  a key the stack itself manages.
- `enable_kms = false` returns to AWS-managed encryption. Because a
  wrong key policy fails silently, the key is rolled out in its own
  apply, after the rest of the stack is verified, and smoke-tested on
  its own.
- The queues set `sqs_managed_sse_enabled` only when the key is off
  (`null` otherwise): the AWS provider rejects the attribute next to
  `kms_master_key_id`, even as `false`, and `terraform validate` does
  not catch it. The first rollout stopped on that error after the
  buckets, table, topic and log groups had switched to the key but
  before the processor's role could use it, so for about two minutes
  an upload would have failed and waited in SQS for a retry. The
  second apply finished the switch, and the smoke test passed.

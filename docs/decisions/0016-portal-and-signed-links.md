# 0016. A token-protected portal behind API Gateway; email links are HMAC-signed

Status: accepted (2026-10-05)

## Context

Uploading needed AWS credentials, and notification emails carried
`s3://` paths that only AWS users can open. S3 presigned URLs created by
the Lambda are signed with its temporary credentials and stop working
when those expire, often within hours, whatever expiry they were given.

## Decision

- `src/file_pipeline/portal.py` serves three routes behind an API
  Gateway HTTP API:
  - `POST /uploads` returns a presigned POST for one new key under
    `uploads/` (10 MiB max, 15 minutes);
  - `GET /jobs/...` returns status and fresh 5-minute download links;
  - `GET /r/{job}/{file}` checks an HMAC-SHA256 signature over job, file
    and expiry, then redirects to a fresh 5-minute S3 URL.
- Email links are signed for 7 days (`report_link_days`).
- The upload token is random (Terraform); the function holds only its
  SHA-256 and compares in constant time. `tools/upload.html` is a local
  page, not hosted anywhere.
- API Gateway, not a public Lambda function URL. Since October 2025 a
  public function URL needs a second permission conditioned on
  `lambda:InvokedViaFunctionUrl`, which the pinned AWS provider (5.100)
  cannot express; without the condition anyone could invoke the
  function directly. API Gateway also rate-limits and writes access logs
  (without query strings, which carry signatures).

## Consequences

- Anyone holding an email can download that job's reports for 7 days.
  That is the point of the link, but treat the emails accordingly.
- Secrets live in Terraform state (the encrypted, private state bucket)
  and in the functions' environment (encrypted with the project key,
  [0017](0017-customer-managed-key.md)).
- The portal's role can only create objects under `uploads/`, read
  reports, and read job records.

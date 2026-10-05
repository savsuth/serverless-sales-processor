# 0018. CI enforces types, coverage, dependency audit and a Terraform scan

Status: accepted (2026-10-05)

## Context

CI ran ruff, pytest, `terraform fmt` and `validate`. Nothing checked
types, coverage, known-vulnerable dependencies, or Terraform security
settings.

## Decision

The CI pipeline runs:

- mypy (configured in `pyproject.toml`);
- pytest with a 90% coverage floor;
- pip-audit;
- checkov over `infra/` with `.checkov.yaml`.

Terraform is installed in the Python job, so the Terraform-vs-Python
column test runs there. Dependabot proposes weekly updates for Python
packages, Actions, and both Terraform roots. `scripts/smoke_test.py`
(`make smoke`) checks a deployed stack end to end.

checkov exceptions are written down with their reasons rather than
silenced. Repo-wide ones are in `.checkov.yaml`:

- no VPC needed;
- no reserved concurrency ([0002](0002-sqs-between-s3-and-lambda.md));
- no Lambda DLQ for non-async functions;
- no code signing;
- no cross-region replication, which the SCP forbids;
- no S3 access logs;
- 30-day log retention.

Per-resource exceptions are inline `checkov:skip` comments: key policy
`*`, disposable Athena results, routes authorized in code, and the state
bucket on SSE-S3.

## Consequences

- A new checkov rule or a newly reported vulnerability can fail CI
  without any code change; that is intended.
- The smoke test uploads real files to a live stack and leaves them as
  job history.

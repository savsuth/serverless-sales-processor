# 0006. Terraform is split into a bootstrap root and an application root

Status: accepted (original build; recorded 2026-10-04)

## Context

Terraform's remote state needs an S3 bucket that must exist before the
stack that uses it, so it cannot be created by that stack. The optional
GitHub Actions deploy role also must not be able to change its own
permissions.

## Decision

- `infra/bootstrap` uses local state and creates the state bucket
  (versioned, encrypted, TLS-only) and, optionally, the GitHub OIDC
  deploy role.
- `infra` is the application stack. It uses that bucket as an S3
  backend with S3-native locking (`use_lockfile`, Terraform 1.10 or
  later), so no DynamoDB lock table is needed.

## Consequences

- The deploy role is managed outside the stack it deploys, so a deploy
  can never widen its own permissions. Every new AWS service in the
  application stack (for example Glue and Athena in
  [0010](0010-curated-json-lines-for-athena.md)) needs a matching grant
  added to `infra/bootstrap/github_oidc.tf` and applied from a laptop.
- `aws_region` and `project_name` must match in both roots.
- The bootstrap state file lives on the machine that applied it.

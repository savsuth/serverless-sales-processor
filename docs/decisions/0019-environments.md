# 0019. Environments are separate stacks, selected by prefix and state file

Status: accepted (2026-10-05)

## Context

Every change was tested directly on the only deployed stack.

## Decision

A dev stack is a full copy with its own state key
(`csv-sales-pipeline-dev/terraform.tfstate`) and its own `project_name`,
which prefixes every resource name. Templates for both live in
`infra/envs/` (the real files are gitignored). `make init ENV=dev` and
`make plan ENV=dev` select it. `make plan` refuses to run when Terraform
is initialised for another environment's state
(`scripts/check_tf_backend.py`). Every resource carries an `Environment`
tag.

## Consequences

- Doubling the stack doubles its fixed costs (KMS key, any idle
  charges); usage costs follow usage. No dev stack is deployed by
  default.
- The optional GitHub deploy role is scoped to the prefix it was created
  with; dev stacks deploy from a laptop.

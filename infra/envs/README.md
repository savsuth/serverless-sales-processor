# Environments

`prod` is the default: `infra/backend.hcl` plus `infra/terraform.tfvars`.

Another environment is a complete second copy of the stack with its own
state file and its own resource names (every name starts with
`project_name`). For `dev`:

1. Copy `dev.backend.hcl.example` to `dev.backend.hcl` and
   `dev.tfvars.example` to `dev.tfvars` (both gitignored) and fill in the
   state bucket name.
2. From the repository root: `make init ENV=dev`, then `make plan ENV=dev`,
   review, and `make apply`.
3. Switch back with `make init` (prod).

`make plan` refuses to run when Terraform's current backend does not
belong to the requested `ENV`, so a dev plan can never be made against
prod's state or the other way round.

The optional GitHub deploy role (`infra/bootstrap`) is scoped to the
`project_name` prefix it was created with; a dev stack is deployed from
a laptop.

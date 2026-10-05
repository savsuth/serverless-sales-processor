variable "project_name" {
  description = "Must match the main stack's project_name; the deploy role's permissions are scoped by this prefix."
  type        = string
  default     = "csv-sales-pipeline"
}

variable "aws_region" {
  description = "Region for the state bucket and every resource the deploy role may manage. Must match the main stack's aws_region (default us-east-2 -- see that variable's description for why)."
  type        = string
  default     = "us-east-2"
}

variable "state_key" {
  description = "Object key of the main stack's state file in the state bucket (must match backend.hcl)."
  type        = string
  default     = "csv-sales-pipeline/terraform.tfstate"
}

variable "enable_github_oidc" {
  description = "Create the GitHub Actions OIDC deploy role. Leave false for laptop-only deploys."
  type        = bool
  default     = false
}

variable "github_repository" {
  description = "Repository allowed to assume the deploy role, as \"owner/repo\". Required when enable_github_oidc is true."
  type        = string
  default     = ""

  validation {
    condition     = var.github_repository == "" || can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "github_repository must look like \"owner/repo\" (no wildcards)."
  }
}

variable "github_environment" {
  description = "GitHub Environment whose jobs may assume the deploy role. Configure required reviewers and a deployment-branch rule (master only) on that Environment: GitHub puts the environment, not the branch, in the OIDC subject claim for jobs that use one."
  type        = string
  default     = "production"
}

variable "existing_github_oidc_provider_arn" {
  description = "An account can have only one OIDC provider per issuer URL. If token.actions.githubusercontent.com is already registered, set its ARN here and it will be reused instead of created."
  type        = string
  default     = ""
}

variable "project_tag" {
  description = "Value of the Project tag the application stack's provider applies to every resource (infra/providers.tf); KMS permissions for the deploy role are scoped to keys carrying it."
  type        = string
  default     = "csv-sales-pipeline"
}

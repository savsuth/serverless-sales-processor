terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.67"
    }
  }

  # Intentionally local state. This root creates the bucket that holds the
  # main stack's state, so it cannot itself live there until it exists.
  # Its state is small (one bucket + optional OIDC role) and gitignored --
  # treat terraform.tfstate here as sensitive and keep it on your machine.
  # If you lose it, `terraform import` the bucket and role back in.
}

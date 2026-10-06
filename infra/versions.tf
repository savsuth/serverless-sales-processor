terraform {
  required_version = ">= 1.10" # S3-native state locking (use_lockfile)

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.67"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State lives in an S3 bucket created by infra/bootstrap (versioned,
  # encrypted, private, TLS-only) with S3-native locking, so no DynamoDB
  # lock table is needed. Bucket/key/region are deliberately NOT written
  # here -- supply them at init time so the same code works for any
  # account:
  #
  #   terraform init -backend-config=backend.hcl
  #
  # (copy backend.hcl.example -> backend.hcl; it is gitignored). CI runs
  # `terraform init -backend=false`, which skips this entirely.
  backend "s3" {
    encrypt      = true
    use_lockfile = true
  }
}

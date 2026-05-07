terraform {
  required_version = ">= 1.6.0"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 2.0.0"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }

  # S3 backend (uncomment and remove backend "local" block above to use):
  # backend "s3" {
  #   bucket = "clickhouse-lab-tf-state"
  #   key    = "snowflake/migration-lab/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

# Authentication note:
# For CI/CD pipelines, replace `password` with `private_key_path`:
#   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_rsa_key.p8 -nocrypt
#   openssl rsa -in snowflake_rsa_key.p8 -pubout -out snowflake_rsa_key.pub
#   ALTER USER TERRAFORM_SVC SET RSA_PUBLIC_KEY='<contents of snowflake_rsa_key.pub>';

# ACCOUNTADMIN is required to:
#   - Create account-level roles (SECURITYADMIN privilege)
#   - Create resource monitors (ACCOUNTADMIN-only)
#   - Grant roles to users (MANAGE GRANTS privilege)
# Database objects (database, schema, warehouse) are created by ACCOUNTADMIN and
# immediately transferred to SYSADMIN ownership so SQL scripts work as SYSADMIN.
provider "snowflake" {
  organization_name = var.snowflake_org
  account_name      = var.snowflake_account
  user              = var.snowflake_user
  password          = var.snowflake_password # use private_key_path for CI/CD
  role              = "ACCOUNTADMIN"
}

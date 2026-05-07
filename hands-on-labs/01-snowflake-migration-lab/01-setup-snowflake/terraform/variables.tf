variable "snowflake_org" {
  description = "Snowflake organization name (the part before the account in the account identifier)."
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account name (the part after the organization in the account identifier)."
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake user that Terraform authenticates as. Should be a dedicated service account."
  type        = string
  default     = "TERRAFORM_SVC"
}

variable "snowflake_password" {
  description = "Password for the Snowflake service account. Use an empty string when authenticating via private key."
  type        = string
  sensitive   = true
  default     = ""
}

variable "snowflake_private_key_path" {
  description = "Path to the PKCS#8 RSA private key file used for key-pair authentication. Leave empty when using password auth."
  type        = string
  default     = ""
}

variable "environment" {
  description = "Deployment environment label (e.g. lab, dev, prod). Used in comments and tags."
  type        = string
  default     = "lab"
}

variable "lab_cohort" {
  description = "Workshop cohort identifier embedded in resource comments for cost attribution and lifecycle tracking."
  type        = string
  default     = "fy27-q1"
}

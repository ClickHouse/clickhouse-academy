variable "clickhouse_org_id" {
  description = "ClickHouse Cloud organization ID"
  type        = string
}

variable "clickhouse_token_key" {
  description = "ClickHouse Cloud API token key"
  type        = string
  sensitive   = true
}

variable "clickhouse_token_secret" {
  description = "ClickHouse Cloud API token secret"
  type        = string
  sensitive   = true
}

variable "clickhouse_password" {
  description = "Password for the ClickHouse service default user"
  type        = string
  sensitive   = true
}

variable "cohort" {
  description = "Lab cohort identifier used in resource naming"
  type        = string
  default     = "fy27-q1"
}

variable "cloud_provider" {
  description = "Cloud provider for ClickHouse Cloud service"
  type        = string
  default     = "aws"
}

variable "region" {
  description = "Region for ClickHouse Cloud service"
  type        = string
  default     = "us-east-1"
}

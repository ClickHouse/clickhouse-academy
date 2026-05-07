terraform {
  required_version = ">= 1.6"
  required_providers {
    clickhouse = {
      source  = "ClickHouse/clickhouse"
      version = "~> 2.0"
    }
  }
}

provider "clickhouse" {
  organization_id = var.clickhouse_org_id
  token_key       = var.clickhouse_token_key
  token_secret    = var.clickhouse_token_secret
}

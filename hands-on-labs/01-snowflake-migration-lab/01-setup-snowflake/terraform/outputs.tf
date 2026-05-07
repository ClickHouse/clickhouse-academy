output "database_name" {
  description = "Name of the NYC Taxi migration database."
  value       = snowflake_database.nyc_taxi.name
}

output "transform_warehouse_name" {
  description = "Name of the transformation warehouse used by dbt and ELT pipelines."
  value       = snowflake_warehouse.transform_wh.name
}

output "analytics_warehouse_name" {
  description = "Name of the analytics warehouse used by BI tools and ad-hoc queries."
  value       = snowflake_warehouse.analytics_wh.name
}

output "raw_schema" {
  description = "Fully-qualified name of the RAW schema."
  value       = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
}

output "staging_schema" {
  description = "Fully-qualified name of the STAGING schema."
  value       = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.staging.name}"
}

output "analytics_schema" {
  description = "Fully-qualified name of the ANALYTICS schema."
  value       = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
}

output "connection_info" {
  description = "Human-readable summary of the provisioned Snowflake environment."
  value       = <<-EOT
    Snowflake Environment — ${var.environment} / cohort: ${var.lab_cohort}
    ---------------------------------------------------------------
    Organization   : ${var.snowflake_org}
    Account        : ${var.snowflake_account}
    Database       : ${snowflake_database.nyc_taxi.name}
    Schemas        : ${snowflake_schema.raw.name}, ${snowflake_schema.staging.name}, ${snowflake_schema.analytics.name}
    Warehouses     : ${snowflake_warehouse.transform_wh.name} (SMALL), ${snowflake_warehouse.analytics_wh.name} (MEDIUM)
    Roles          : ${snowflake_account_role.transformer.name}, ${snowflake_account_role.analyst.name}, ${snowflake_account_role.dbt.name}, ${snowflake_account_role.loader.name}
    Resource Monitor: ${snowflake_resource_monitor.analytics_wh_monitor.name}
  EOT
}

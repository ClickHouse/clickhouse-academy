resource "snowflake_database" "nyc_taxi" {
  name    = "NYC_TAXI_DB"
  comment = "NYC Taxi migration lab — source Snowflake environment"
}

# Transfer database and schema ownership to SYSADMIN so SQL scripts and
# dbt (which use SYSADMIN-owned objects) have full DDL access.
resource "snowflake_grant_ownership" "nyc_taxi_db_to_sysadmin" {
  account_role_name = "SYSADMIN"
  on {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc_taxi.name
  }
  outbound_privileges = "COPY"
  depends_on          = [snowflake_database.nyc_taxi]
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.nyc_taxi.name
  name     = "RAW"
  comment  = "Raw ingested data — immutable source of truth"
}

resource "snowflake_grant_ownership" "raw_schema_to_sysadmin" {
  account_role_name   = "SYSADMIN"
  outbound_privileges = "COPY"
  on {
    object_type = "SCHEMA"
    object_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
  }
  depends_on = [snowflake_schema.raw]
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.nyc_taxi.name
  name     = "STAGING"
  comment  = "dbt staging models — cleaned and typed"
}

resource "snowflake_grant_ownership" "staging_schema_to_sysadmin" {
  account_role_name   = "SYSADMIN"
  outbound_privileges = "COPY"
  on {
    object_type = "SCHEMA"
    object_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.staging.name}"
  }
  depends_on = [snowflake_schema.staging]
}

resource "snowflake_schema" "analytics" {
  database = snowflake_database.nyc_taxi.name
  name     = "ANALYTICS"
  comment  = "Dimensional model — served to BI tools"
}

resource "snowflake_grant_ownership" "analytics_schema_to_sysadmin" {
  account_role_name   = "SYSADMIN"
  outbound_privileges = "COPY"
  on {
    object_type = "SCHEMA"
    object_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
  }
  depends_on = [snowflake_schema.analytics]
}

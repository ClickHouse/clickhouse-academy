# ---------------------------------------------------------------------------
# Role definitions (provider v2.x: snowflake_account_role)
# ---------------------------------------------------------------------------

resource "snowflake_account_role" "transformer" {
  name    = "TRANSFORMER_ROLE"
  comment = "Runs ELT pipelines and dbt staging models — lab cohort: ${var.lab_cohort}"
}

resource "snowflake_account_role" "analyst" {
  name    = "ANALYST_ROLE"
  comment = "Read-only access to the ANALYTICS schema for BI tools — lab cohort: ${var.lab_cohort}"
}

resource "snowflake_account_role" "dbt" {
  name    = "DBT_ROLE"
  comment = "Full dbt service account role — reads RAW, writes STAGING and ANALYTICS — lab cohort: ${var.lab_cohort}"
}

resource "snowflake_account_role" "loader" {
  name    = "LOADER_ROLE"
  comment = "Ingestion service account role — loads raw data into RAW schema — lab cohort: ${var.lab_cohort}"
}

# ---------------------------------------------------------------------------
# SYSADMIN ownership of custom roles
# ---------------------------------------------------------------------------

resource "snowflake_grant_account_role" "sysadmin_owns_transformer" {
  role_name        = snowflake_account_role.transformer.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "sysadmin_owns_analyst" {
  role_name        = snowflake_account_role.analyst.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "sysadmin_owns_dbt" {
  role_name        = snowflake_account_role.dbt.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "sysadmin_owns_loader" {
  role_name        = snowflake_account_role.loader.name
  parent_role_name = "SYSADMIN"
}

# ---------------------------------------------------------------------------
# Grant all lab roles to the lab user so they can switch roles in the UI
# and dbt / the producer can connect with the right role.
# Requires ACCOUNTADMIN (MANAGE GRANTS privilege).
# ---------------------------------------------------------------------------

resource "snowflake_grant_account_role" "user_gets_transformer" {
  role_name = snowflake_account_role.transformer.name
  user_name = var.snowflake_user
}

resource "snowflake_grant_account_role" "user_gets_analyst" {
  role_name = snowflake_account_role.analyst.name
  user_name = var.snowflake_user
}

resource "snowflake_grant_account_role" "user_gets_dbt" {
  role_name = snowflake_account_role.dbt.name
  user_name = var.snowflake_user
}

resource "snowflake_grant_account_role" "user_gets_loader" {
  role_name = snowflake_account_role.loader.name
  user_name = var.snowflake_user
}


# ===========================================================================
# TRANSFORMER_ROLE grants
# ===========================================================================

resource "snowflake_grant_privileges_to_account_role" "transformer_database" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc_taxi.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_future_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_staging_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.staging.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_staging_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.staging.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_staging_future_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.staging.name}"
    }
  }
}

# ===========================================================================
# ANALYST_ROLE grants
# ===========================================================================

resource "snowflake_grant_privileges_to_account_role" "analyst_database" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc_taxi.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_warehouse" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.analytics_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_analytics_schema" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_analytics_tables" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_analytics_future_tables" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_analytics_views" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "VIEWS"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_analytics_future_views" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
    }
  }
}

# ===========================================================================
# DBT_ROLE grants
# ===========================================================================

resource "snowflake_grant_privileges_to_account_role" "dbt_database" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc_taxi.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_transform_warehouse" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_warehouse" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.analytics_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_schema" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_tables" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_future_tables" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_staging_schema" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE STAGE"]
  on_schema {
    schema_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.staging.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_staging_tables" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.staging.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_staging_future_tables" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.staging.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_schema" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE STAGE"]
  on_schema {
    schema_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_tables" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_future_tables" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.analytics.name}"
    }
  }
}

# ===========================================================================
# LOADER_ROLE grants
# ===========================================================================

resource "snowflake_grant_privileges_to_account_role" "loader_database" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc_taxi.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_warehouse" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw_schema" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE"]
  on_schema {
    schema_name = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw_tables" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["INSERT", "SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw_future_tables" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["INSERT", "SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.nyc_taxi.name}.${snowflake_schema.raw.name}"
    }
  }
}

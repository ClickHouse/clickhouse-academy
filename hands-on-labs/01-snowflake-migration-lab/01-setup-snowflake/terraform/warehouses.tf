resource "snowflake_warehouse" "transform_wh" {
  name           = "TRANSFORM_WH"
  warehouse_size = "SMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Used by dbt and ELT pipelines — lab cohort: ${var.lab_cohort}"
}

resource "snowflake_warehouse" "analytics_wh" {
  name             = "ANALYTICS_WH"
  warehouse_size   = "SMALL"
  auto_suspend     = 60
  auto_resume      = true
  resource_monitor = snowflake_resource_monitor.analytics_wh_monitor.name
  comment          = "Used by BI tools and ad-hoc analyst queries — lab cohort: ${var.lab_cohort}"
}

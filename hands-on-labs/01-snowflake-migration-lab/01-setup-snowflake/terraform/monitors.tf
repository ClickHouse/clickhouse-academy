resource "snowflake_resource_monitor" "analytics_wh_monitor" {
  name         = "ANALYTICS_WH_MONITOR"
  credit_quota = 50
  frequency    = "MONTHLY"

  # IMMEDIATELY means the monitor starts tracking from the moment it is created.
  start_timestamp = "IMMEDIATELY"

  # Send an email notification to account admins at 75% consumption.
  notify_triggers = [75]

  # Suspend the warehouse (but allow current queries to finish) at 100% consumption.
  suspend_trigger = 100
}

resource "clickhouse_service" "nyc_taxi" {
  name           = "nyc-taxi-lab-${var.cohort}"
  cloud_provider = var.cloud_provider
  region         = var.region
  # tier is only required for legacy ClickHouse Cloud organizations.
  # Omit it for organizations on the new ClickHouse Cloud tiers (the default for all new sign-ups).

  idle_scaling        = true
  idle_timeout_minutes = 15

  password = var.clickhouse_password

  # Lab-only: open access avoids per-partner IP whitelisting friction.
  # Restrict to your office CIDR for production use.
  ip_access = [
    {
      source      = "0.0.0.0/0"
      description = "lab-open-access"
    }
  ]
}

output "clickhouse_host" {
  description = "ClickHouse Cloud service hostname (HTTPS endpoint)"
  value       = [for ep in clickhouse_service.nyc_taxi.endpoints : ep.host if ep.protocol == "https"][0]
}

output "clickhouse_port" {
  description = "ClickHouse Cloud HTTPS port (always 8443 for Cloud services)"
  value       = 8443
}

output "service_id" {
  description = "ClickHouse Cloud service ID"
  value       = clickhouse_service.nyc_taxi.id
}

output "service_name" {
  description = "ClickHouse Cloud service name"
  value       = clickhouse_service.nyc_taxi.name
}

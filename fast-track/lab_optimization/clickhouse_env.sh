#!/bin/bash

# Required settings
export CLICKHOUSE_HOST=my-host.my-service.clickhouse.cloud 
export CLICKHOUSE_PASSWORD=my_password

# Optional settings
export CLICKHOUSE_USER=default
export CLICKHOUSE_DATABASE=training
export CLICKHOUSE_PORT=9440
# use port 9440 if using ClickHouse Cloud or 9000 if running locally

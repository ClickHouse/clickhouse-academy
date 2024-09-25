#!/bin/bash

# Define ClickHouse connection parameters
CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PORT="9000"
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""  # Use your actual password or pass as an environment variable

# SQL file containing ClickHouse commands
SQL_FILE="setup.sql"

# Execute the commands from the SQL file
echo clickhouse client \
  --host $CLICKHOUSE_HOST \
  --port $CLICKHOUSE_PORT \
  --user $CLICKHOUSE_USER \
  --password "$CLICKHOUSE_PASSWORD" \
  --echo \
  --progress \
  -n < $SQL_FILE

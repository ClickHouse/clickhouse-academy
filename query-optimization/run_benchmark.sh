#!/bin/bash

# Requires environment variables:
# CLICKHOUSE_HOST=my-host.my-service.clickhouse.cloud
# CLICKHOUSE_PASSWORD=MyPaSsWoRd
# (will prompt for password if unspecified)
# CLICKHOUSE_DATABASE=my_db
# (uses default if unspecified)
# CLICKHOUSE_USER=user-name
# (uses default if unspecified)
# CLICKHOUSE_PORT=1234
# (uses ClickHouse Cloud default port 9440 if unspecified - set to 9000 if running locally)

# Usage:
# run_benchmark.sh [table-name] [query-files]

# Which percentile times to display?
PERCENTILE=80.0
# How much iterations of each query?
ITERATIONS=15

# clickhouse-benchmark options
OPTIONS="--secure --iterations=$ITERATIONS --delay=0 --enable_filesystem_cache=0"

host=${CLICKHOUSE_HOST}
db=${CLICKHOUSE_DATABASE:-default}
user=${CLICKHOUSE_USER:-default}
port=${CLICKHOUSE_PORT:-9440}

echo "Connecting to $user@$host:$port, $db database "

pwd=$CLICKHOUSE_PASSWORD
if [ -z "$pwd" ]; then 
   read -s -p "Please enter password for $user@$host:$port: " pwd
fi

if [ $# -gt 1 ]
then
    test_table=$1
    query_files=$2
elif [ $# -eq 1 ]
then 
    echo hi
    test_table=$1
    query_files=`ls benchmark_query_*.sql`
else
    test_table='nyc_taxi'
    query_files=`ls benchmark_query_*.sql`
fi 

for query_file in $query_files
do
  echo "*********************************************************************"
  echo "**** Running $query_file on $db.$test_table ( $ITERATIONS iterations ):"
  query=$(cat $query_file | sed "s/\$TABLE/$test_table/g")
  echo "**** clickhouse benchmark $OPTIONS --host $host --password $pwd --port $port --database $db --query $query *****"
  clickhouse benchmark $OPTIONS --host $host --password $pwd --port $port --database $db --query "$query"
done

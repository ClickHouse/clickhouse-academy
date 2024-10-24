#!/bin/bash

# Required environment variable:
# CLICKHOUSE_HOST=my-host.my-service.clickhouse.cloud

# Optional environment variables:
# CLICKHOUSE_PASSWORD=MyPaSsWoRd
# (will prompt for password if unspecified)
# CLICKHOUSE_DATABASE=my_db
# (database to connect to; uses default if unspecified)
# CLICKHOUSE_USER=my_user-name
# (uses default if unspecified)
# CLICKHOUSE_PORT=9440
# (uses ClickHouse Cloud default port 9440 if unspecified - set to 9000 if running locally)
# CLICKHOUSE_QUERY_ITERATIONS=15
# (number of times to run the query -- uses DEFAULT_ITERATIONS if unspecified)


# Usage:
# run_benchmark.sh [table-name] [query-files]
# examples: 
# run_benchmark.sh nyc_taxi_key_1
# run_benchmark.sh nyc_taxi 'benchmark_queries_0[2-4].sql'
# run_benchmark.sh nyc_taxi_inferred 'my_queries/*.sql'

# More iterations give more reliable results but takes longer
DEFAULT_ITERATIONS=10

# clickhouse-benchmark options
OPTIONS="--secure --delay=0 --enable_filesystem_cache=0"

host=${CLICKHOUSE_HOST}
db=${CLICKHOUSE_DATABASE:-default}
user=${CLICKHOUSE_USER:-default}
port=${CLICKHOUSE_PORT:-9440}
iterations=${CLICKHOUSE_QUERY_ITERATIONS:-$DEFAULT_ITERATIONS}

echo "** Connecting to $user@$host:$port, $db database "

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
    test_table=$1
    query_files=`ls benchmark_query_*.sql`
else
    test_table='nyc_taxi'
    query_files=`ls benchmark_query_*.sql`
fi 

echo "** Using query files $query_files with table $db.$test_table"

# loop through specified query files
for query_file in $query_files
do
  echo "*********************************************************************"
  echo "**** Running $query_file on $test_table ( $iterations iterations ):"

  # Read the query file, substitute specified table, replace any newlines with spaces
  query=$(cat $query_file | sed "s/\$TABLE/$test_table/g" | tr -s '\n' ' ')

  # Show user the command but hide password for security
  echo "**** clickhouse benchmark $OPTIONS --host $host --password \$CLICKHOUSE_PASSWORD --port $port --database $db --iterations $iterations --query \"$query\" *****"

  # and go!
  clickhouse benchmark $OPTIONS --host $host --password $pwd --port $port --database $db  --iterations $iterations --query "$query"
done

import subprocess
import statistics
import time as tm
import sys

# Define the mapping of tables to their respective queries.sql files
table_query_mapping = {
    "nyc_taxi": "queries.sql",
    "nyc_taxi_opt3": "queries.sql",
    "nyc_taxi_opt4_1": "queries.sql",
    "nyc_taxi_opt4_2": "queries_opt4_2.sql",
    "nyc_taxi_opt4_3": "queries_opt4_3.sql",
    "nyc_taxi_opt5_1": "queries_opt4_2.sql",
    "nyc_taxi_opt5_2": "queries_opt4_2.sql",
    "nyc_taxi_opt5_3": "queries_opt4_2.sql",
    "nyc_taxi_opt5_4": "queries_opt4_2.sql",
    "nyc_taxi_opt5_5": "queries_opt4_2.sql",
}

# Define the number of runs per query
runs = 5

# Function to run a query and return the execution time
def run_query(table, query):
    query_with_table = query.replace("$TABLE", table)
    query_with_table = query_with_table + " SETTINGS enable_filesystem_cache=0"

    start_time = tm.time()
    # Use subprocess to execute the ClickHouse query and capture the execution time
    result = subprocess.run(
        ['clickhouse', 'client', '-q', query_with_table],
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        text=True
    )
    end_time = tm.time()
    tm.sleep(1) # Optional sleep for consistency between runs
    return end_time - start_time

# Function to run a query multiple times, discard min and max, and calculate the average
def benchmark_query(table, query):
    times = [run_query(table, query) for _ in range(runs)]
    # Sort times, discard min and max
    sorted_times = sorted(times)
    middle_times = sorted_times[1:-1]
    # Calculate average of middle 3 times
    return statistics.mean(middle_times)

# Main function to run the benchmark
def run_benchmark_for_table(table, query_file, query_number=None):
    # Read the queries from the associated query file
    with open(query_file, 'r') as f:
        queries = f.readlines()

    # If query_number is provided, execute only that query
    if query_number:
        query = queries[query_number - 1].strip()
        avg_time = benchmark_query(table, query)
        print(f"Average time for query {query_number}: {avg_time:.3f} sec")
    else:
        # Loop through each query if no query number is provided
        for i, query in enumerate(queries, start=1):
            avg_time = benchmark_query(table, query.strip())
            print(f"Average time for query {i}: {avg_time:.3f} sec")

def main():
    if len(sys.argv) < 2:
        print("Usage: script.py <table_name> [query_file] [query_number]")
        sys.exit(1)

    table_name = sys.argv[1]

    # Case 1: If table_name is "all_tables", run benchmark for all tables in the mapping
    if table_name == "all_tables":
        all_results = []
        for table, query_file in table_query_mapping.items():
            print(f"Running queries for table '{table}' using '{query_file}'")
            run_benchmark_for_table(table, query_file)
            print("=======================================")
    else:
        # Case 2: Specific table name, so query_file is required
        if len(sys.argv) < 3:
            print("For a specific table, you must provide <query_file>.")
            sys.exit(1)

        query_file = sys.argv[2]

        # Optional query_number argument
        if len(sys.argv) == 4:
            query_number = int(sys.argv[3])
            print(f"Running query {query_number} for table '{table_name}' using '{query_file}'")
            run_benchmark_for_table(table_name, query_file, query_number)
        else:
            print(f"Running queries for table '{table_name}' using '{query_file}'")
            run_benchmark_for_table(table_name, query_file)

if __name__ == "__main__":
    main()
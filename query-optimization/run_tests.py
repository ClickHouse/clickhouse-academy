import subprocess
import statistics
import time as tm
import sys

# TODO: Modify for your connection details
user="default"
host="localhost"
port="9000"
password=""
secure = '' if host == 'localhost' else '--secure'

# Define the mapping of tables to their respective queries.sql files
table_query_test_mapping = [
    ("nyc_taxi", "queries.sql"),
    ("nyc_taxi_opt3", "queries.sql"),
    ("nyc_taxi_opt4_1", "queries.sql"),
    ("nyc_taxi_opt4_2", "solutions/queries_opt4_2.sql"),
    ("nyc_taxi_opt5_1", "solutions/queries_opt4_2.sql"),
    ("nyc_taxi_opt5_2", "solutions/queries_opt4_2.sql"),
    ("nyc_taxi_opt5_3", "solutions/queries_opt4_2.sql"),
    ("nyc_taxi_opt5_4", "solutions/queries_opt4_2.sql"),
    ("nyc_taxi_opt5_5", "solutions/queries_opt4_2.sql"),
    ("nyc_taxi_opt5_1", "solutions/queries_opt6_1.sql"),
    ("nyc_taxi_opt5_1", "solutions/queries_opt6_2.sql"),
    ("nyc_taxi_opt5_1", "solutions/queries_opt6_3.sql"),
    ("nyc_taxi_opt5_1", "solutions/queries_opt6_4.sql"),
    ("nyc_taxi_opt7", "solutions/queries_opt7.sql"),
    ("nyc_taxi_opt8", "solutions/queries_opt7.sql"),
    ("nyc_taxi_opt8", "solutions/queries_opt9.sql"),
]

# Define the number of runs per query
runs = 5

# Function to run a query and return the execution time
def run_query(table, query):
    query_with_table = query.replace("$TABLE", table)
    query_with_table = query_with_table + " SETTINGS enable_filesystem_cache=0"

    # connect to ClickHouse
    command = ['clickhouse', 'client', '--host', host, '--port', port, secure, '--password', password, '-q', query_with_table]

    start_time = tm.time()
    # Use subprocess to execute the ClickHouse query and capture the execution time
    result = subprocess.run(
        command,
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        text=True
    )
    end_time = tm.time()

    # Check for errors
    if result.returncode != 0:
        print(f"Error executing query on table '{table}': {result.stderr}")
        raise Exception(f"Query failed with return code {result.returncode}")

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

# Main function to run the tests and collect results
def run_tests_for_table(table, query_file, query_number=None):
    # Read the queries from the associated query file
    with open(query_file, 'r') as f:
        queries = f.readlines()

    table_results = []

    # If query_number is provided, execute only that query
    if query_number:
        query = queries[query_number - 1].strip()
        avg_time = benchmark_query(table, query)
        print(f"Average time for query {query_number}: {avg_time:.3f} sec")
        table_results.append(avg_time)
    else:
        # Loop through each query if no query number is provided
        for i, query in enumerate(queries, start=1):
            avg_time = benchmark_query(table, query.strip())
            print(f"Average time for query {i}: {avg_time:.3f} sec")
            table_results.append(avg_time)

    return table_results

def print_final_results(all_results):
    if not all_results:
      return

    # Print final results in a tabular format
    print("\nFinal results (queries across tables):")
    print("=======================================")
    # Print results in the format: t1_q1 t2_q1 t3_q1 ...
    for query_idx in range(len(all_results[0])):
        row = [f"{all_results[table_idx][query_idx]:.3f}" for table_idx in range(len(all_results))]
        print(" ".join(row))

def main():
    if len(sys.argv) < 2:
        print("Usage: script.py <table_name> [query_file] [query_number]")
        sys.exit(1)

    table_name = sys.argv[1]
    all_results = []

    # Case 1: If table_name is "all_tests", run tests for all steps in the mapping
    if table_name == "all_tests":
        for table, query_file in table_query_test_mapping:
            print(f"Running queries for table '{table}' using '{query_file}'")
            table_results = run_tests_for_table(table, query_file)
            all_results.append(table_results)
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
            run_tests_for_table(table_name, query_file, query_number)
        else:
            print(f"Running queries for table '{table_name}' using '{query_file}'")
            table_results = run_tests_for_table(table_name, query_file)
            all_results.append(table_results)
            print("=======================================")

    # Print final summary of results
    print_final_results(all_results)

if __name__ == "__main__":
    main()

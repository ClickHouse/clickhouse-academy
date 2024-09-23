import subprocess
import statistics
import time

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
    # Use subprocess to execute the ClickHouse query and capture the execution time
    result = subprocess.run(
        ['/usr/bin/time', '-f', '%e', 'clickhouse', 'client', '-q', query_with_table],
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        text=True
    )
    return float(result.stderr.strip())

# Function to run a query multiple times, discard min and max, and calculate the average
def benchmark_query(table, query):
    times = [run_query(table, query) for _ in range(runs)]
    # Sort times, discard min and max
    sorted_times = sorted(times)
    middle_times = sorted_times[1:-1]
    # Calculate average of middle 3 times
    return statistics.mean(middle_times)

# Main function to run the benchmark for each table and query
def main():
    all_results = []

    # Loop through each table and its associated query file
    for table, query_file in table_query_mapping.items():
        print(f"Running queries for table: {table} using {query_file}")
        table_results = []

        # Read the queries from the associated query file
        with open(query_file, 'r') as f:
            queries = f.readlines()

        # Loop through each query
        for query in queries:
            avg_time = benchmark_query(table, query.strip())
            table_results.append(avg_time)
            print(f"Average time for query: {avg_time:.3f} sec")

            # Add a small delay between query executions (optional)
            time.sleep(1)

        all_results.append(table_results)
        print("=======================================")

    # Print final results in a tabular format
    print("Final results (queries across tables):")
    print("=======================================")
    # Print results in the format: t1_q1 t2_q1 t3_q1 ...
    for query_idx in range(len(queries)):
        row = [f"{all_results[table_idx][query_idx]:.3f}" for table_idx in range(len(table_query_mapping))]
        print(" ".join(row))

if __name__ == "__main__":
    main()

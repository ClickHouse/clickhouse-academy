-- Step 1
stream_result = sess.send_query("SELECT * FROM Python(taxi_query)", "CSV")

-- Step 2:
import time
  
start_time = time.time()
stream_result = sess.send_query("SELECT * FROM Python(taxi_query)", "CSV")
end_time = time.time()
print(f"Streaming query execution time: {end_time - start_time:.4f} seconds")

start_time = time.time()
stream_result = chdb.query("SELECT * FROM Python(taxi_query)", "CSV")
end_time = time.time()
print(f"Regular query execution time: {end_time - start_time:.4f} seconds")

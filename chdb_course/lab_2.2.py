import pyarrow.parquet as pq
import chdb

# Step 2:
arrow_table = pq.read_table("./2024-04-01_performance_mobile_tiles.parquet")

# Step 3:
result = chdb.query("""
 SELECT count(*)
 FROM Python(arrow_table)
""", "Vertical")
print(result)

# Step 4:
result = chdb.query("""
 SELECT AVG(avg_d_kbps)
 FROM Python(arrow_table)
""", "Vertical")
print(result)


-- Step 2:
import pyarrow.parquet as pq
import chdb
arrow_table = pq.read_table("./2024-04-01_performance_mobile_tiles.parquet")

-- Step 3:
chdb.query("""
 SELECT count(*)
 FROM Python(arrow_table)
""", "Vertical")

Answer: 3703161

-- Step 4:
chdb.query("""
 SELECT AVG(avg_d_kbps)
 FROM Python(arrow_table)
""", "Vertical")

Answer: 108296.14 kb/s

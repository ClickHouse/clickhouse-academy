-- Step 2:
aws s3 cp \
  --no-sign \
  s3://ookla-open-data/parquet/performance/type=mobile/year=2024/quarter=2/2024-04-01_performance_mobile_tiles.parquet .

-- Step 3:
import pyarrow.parquet as pq
arrow_table = pq.read_table("./2024-04-01_performance_mobile_tiles.parquet")

-- Step 4:
chdb.query("""
 SELECT count(*)
 FROM Python(arrow_table)
""", "Vertical")

Answer: 3703161

-- Step 5:
chdb.query("""
 SELECT AVG(avg_d_kbps)
 FROM Python(arrow_table)
""", "Vertical")

Answer: 108296.14 kb/s

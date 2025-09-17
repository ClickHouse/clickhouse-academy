-- Step 1:
import chdb

query = """
SELECT * EXCEPT(columns, row_groups)
FROM s3(
  'https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2013.snappy.parquet', 
  ParquetMetadata
)
"""

print(chdb.query(query, 'Vertical'))

Answer: 28,034,255 rows, 30 row groups

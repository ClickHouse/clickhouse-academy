-- Step 1:
chdb.query("""
SELECT
    _file,
    _path
FROM s3('s3://datasets-documentation/amazon_reviews/*.parquet', One)
SETTINGS output_format_pretty_row_numbers=0
""", 'PrettyCompact')

-- Step 2:
(chdb.query("""
SELECT product_title, COUNT(*) as verified_purchase_count
FROM s3('s3://datasets-documentation/amazon_reviews/amazon_reviews_2014.snappy.parquet’)
WHERE verified_purchase = TRUE
GROUP BY product_title
ORDER BY verified_purchase_count DESC
LIMIT 10
""", 'PrettyCompact'))

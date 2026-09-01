-- Step 1
-- Count the rows in the source Parquet file without ingesting it yet.
-- ClickHouse can query files in S3 directly via the s3() table function.
SELECT formatReadableQuantity(count())
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet');
-- Expected: ~41.2 million rows (this 2015 file only, not the full multi-year dataset).

-- Step 2
-- Preview 100 rows to see the shape of the data before committing to a schema.
SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
LIMIT 100;
-- Expected columns: review_date, marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body.

-- Step 3
-- DESC infers a schema directly from the Parquet file's own metadata —
-- no manual column list needed.
DESC s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet');
-- Expected: columns may all show as Nullable(...) (e.g. Nullable(Int64) for star_rating) — an inference default, not a property of the data.

-- Step 4
-- Turn off that nullable-by-default behavior so the inferred types match
-- what the data actually needs.
DESC s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
SETTINGS schema_inference_make_columns_nullable=0;
-- Expected: plain types now (e.g. review_date Date32) instead of Nullable(...) — needed since PRIMARY KEY columns can't be Nullable, or Step 5 fails with Code: 44.

-- Step 5
-- Create the table using the inferred (non-nullable) schema.
CREATE TABLE reviews
ENGINE = MergeTree
PRIMARY KEY review_date
EMPTY AS
    SELECT *
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
    LIMIT 1
    SETTINGS schema_inference_make_columns_nullable=0;
-- EMPTY builds the schema without inserting the LIMIT 1 row — required since plain CREATE TABLE ... AS SELECT isn't allowed in ClickHouse Cloud.

-- Step 6
SHOW CREATE TABLE reviews;

-- Step 7
-- Now do the real ingest: all ~41.2M rows, not just the LIMIT 1 preview.
INSERT INTO reviews
    SELECT *
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet');
-- Expected: a few minutes to complete — ClickHouse rips through S3 Parquet ingest at ~300K rows/sec, so all ~41.2M rows land fast (later labs cover using s3Cluster to parallelize ingest across your cluster's nodes for even faster loads).

-- Step 8
SELECT formatReadableQuantity(count()) FROM reviews;
-- Expected: ~41.2 million again, confirming every row from the source file landed.

-- Step 9
-- Compare the source Parquet's size against the MergeTree table's
-- on-disk footprint.
SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE table = 'reviews' AND active = 1;
-- Expected: compressed_size noticeably smaller than the 8.6GiB source Parquet (exact figure will vary by version/instance); uncompressed_size larger than both.

-- Step 10
-- A few queries to explore the dataset — watch how much slower the text-search ones are; that's the gap a text index (Module 8) fixes.

-- Total number of votes per category
SELECT
    sum(total_votes),
    product_category
FROM reviews
GROUP BY product_category
ORDER BY 1 DESC;

-- Products with "awful" in the review
SELECT
    product_id,
    any(product_title),
    avg(star_rating),
    count() AS count
FROM reviews
WHERE position(review_body, 'awful') > 0
GROUP BY product_id
ORDER BY count DESC
LIMIT 50;

-- Products with "awesome" in the review
SELECT
    product_id,
    any(product_title),
    avg(star_rating),
    count() AS count
FROM reviews
WHERE position(review_body, 'awesome') > 0
GROUP BY product_id
ORDER BY count DESC
LIMIT 50;
-- Expected: both text-search queries take noticeably longer than the category aggregation, since position() scans every review_body value with no index to skip.

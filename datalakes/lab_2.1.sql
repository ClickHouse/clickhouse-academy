-- Step 1
DESCRIBE icebergS3(
  'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/'
);

-- Step 2 
SELECT *
FROM icebergS3(
  'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/',
  NOSIGN
)
LIMIT 5;

-- Step 3
CREATE TABLE IF NOT EXISTS nyc_taxi_iceberg
ENGINE = IcebergS3(
  'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/',
  NOSIGN
);

-- Step 4
SHOW CREATE TABLE nyc_taxi_iceberg;

-- Step 5
SELECT
  snapshot_id,
  made_current_at,
  parent_id,
  is_current_ancestor
FROM system.iceberg_history
WHERE database = 'default'
AND table = 'nyc_taxi_iceberg'
ORDER BY made_current_at;

-- Step 6
SELECT count()
FROM nyc_taxi_iceberg
SETTINGS iceberg_snapshot_id = [SNAPSHOT_ID];

SELECT count() FROM nyc_taxi_iceberg;

-- Step 7
SELECT
  _file,
  count()          AS rows_in_file,
  _size            AS file_size_bytes
FROM nyc_taxi_iceberg
GROUP BY _file, _size
ORDER BY rows_in_file DESC;


-- Step 8
SELECT
  count()                  AS trip_count,
  round(avg(total_amount), 2) AS avg_fare
FROM nyc_taxi_iceberg;

SELECT
  count()                  AS trip_count,
  round(avg(total_amount), 2) AS avg_fare
FROM nyc_taxi_iceberg
WHERE pickup_datetime >= '2014-01-01'
AND pickup_datetime < '2015-01-01';






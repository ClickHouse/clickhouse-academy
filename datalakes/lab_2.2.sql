-- Step 1
SELECT
  hostName()  AS node_hostname,
  value       AS cpu_count
FROM clusterAllReplicas('default', system.asynchronous_metrics)
WHERE metric = 'CGroupMaxCPU'
ORDER BY node_hostname;

-- Step 2
SELECT
  count(DISTINCT _file)          AS num_parquet_files,
  formatReadableSize(sum(_size)) AS total_data_size
FROM icebergS3(
  'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/'
);

-- Step 3
SYSTEM DROP FILESYSTEM CACHE;
SYSTEM DROP ICEBERG METADATA CACHE;


SELECT
  toHour(pickup_datetime)    AS hour_of_day,
  payment_type,
  count()                    AS trips,
  round(avg(fare_amount), 2) AS avg_fare,
  round(sum(tip_amount), 2)  AS total_tips
FROM icebergS3(
  'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/',
  NOSIGN
)
GROUP BY hour_of_day, payment_type
ORDER BY hour_of_day, payment_type;

-- Step 4
SELECT DISTINCT cluster
FROM system.clusters
ORDER BY cluster;

-- Step 5
SYSTEM DROP FILESYSTEM CACHE;
SYSTEM DROP ICEBERG METADATA CACHE;

SELECT
  toHour(pickup_datetime)    AS hour_of_day,
  payment_type,
  count()                    AS trips,
  round(avg(fare_amount), 2) AS avg_fare,
  round(sum(tip_amount), 2)  AS total_tips
FROM icebergS3Cluster(
  'default',  
  'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/',
  NOSIGN
)
GROUP BY hour_of_day, payment_type
ORDER BY hour_of_day, payment_type;

-- Step 6
SELECT
  toHour(pickup_datetime)    AS hour_of_day,
  payment_type,
  count()                    AS trips,
  round(avg(fare_amount), 2) AS avg_fare,
  round(sum(tip_amount), 2)  AS total_tips
FROM icebergS3Cluster(
  'default',  
  'https://storage.googleapis.com/biglake-public-nyc-taxi-iceberg/public_data/nyc_taxicab/',
  NOSIGN
)
GROUP BY hour_of_day, payment_type
ORDER BY hour_of_day, payment_type;


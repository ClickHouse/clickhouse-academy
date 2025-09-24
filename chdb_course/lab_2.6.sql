-- Step 2
import pandas as pd

data = {
    'trip_id': [1199999922, 1199999929],
    'pickup_datetime': ['2015-07-07 20:38:03', '2015-07-07 20:45:08'],
    'dropoff_datetime': ['	2015-07-07 20:50:13', '2015-07-07 21:06:52'],
    'pickup_longitude': [-73.97434997558594, -73.96578216552734],
    'pickup_latitude': [40.7622184753418, 40.75436019897461],
    'dropoff_longitude': [-73.98577117919922, -73.85907745361328],
    'dropoff_latitude': [40.73539352416992, 40.728702545166016],
    'passenger_count': [1, 1],
    'trip_distance': [2.29, 6.8],
    'fare_amount': [10.5,22.5],
    'extra': [0.5, 0.5],
    'tip_amount': [1.2, 0],
    'tolls_amount': [0, 0],
    'total_amount': [13, 23.8],
    'payment_type': ['CSH', 'CRE'],
    'pickup_ntaname': ['Midtown-Midtown South', 'Turtle Bay-East Midtown'],
    'dropoff_ntaname': ['Gramercy', 'Rego Park']
}

partial_taxi_df = pd.DataFrame(data)



-- Step 3

import chdb

taxi_query = """
WITH 
    parquet_data AS (
        SELECT * FROM file('{parquet_path}', Parquet)
    ),

    s3_data AS (
        SELECT * FROM s3('s3://learn-clickhouse/nyc-taxi/trips0.tsv.gz')
    ),

    df_data AS (
        SELECT * FROM Python(partial_taxi_df)

    )

SELECT * 
FROM parquet_data
FULL JOIN s3_data ON parquet_data.id = s3_data.id
FULL JOIN df_data ON df_data.id = COALESCE(parquet_data.id, s3_data.id)
""".format(parquet_path=parquet_path, s3_path=s3_path)

result = chdb.query(taxi_query, tables={"df": df}, as_pandas=True)

print(result.head(5))

-- Step 4:
query = '''
SELECT *
FROM Python(taxi_df)
WHERE distance = (SELECT MAX(distance) FROM Python(taxi_df))
'''

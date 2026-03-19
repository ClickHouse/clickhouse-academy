# Step 3
import hextoolkit
hex_clickhouse_conn = hextoolkit.get_data_connection('My ClickHouse Connection')
session = hex_clickhouse_conn.get_chdb_session()

# Step 4
from chdb import datastore as pd

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

taxi_df = pd.DataFrame(data)
print(taxi_df.head(10))


# Step 5
session.databases()
session.tables("default")

# Step 6
from chdb import datastore as pd

ds = session.sql("SELECT * FROM default.uk_prices WHERE price > 1000000")
filtered = ds.filter(ds["town"] == "LONDON")
df = filtered.to_pandas()
print(df.head(100))

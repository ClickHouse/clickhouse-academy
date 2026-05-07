#!/usr/bin/env python3
"""
02_migrate_trips.py — Batch migrate NYC Taxi trips from Snowflake to ClickHouse.

Usage:
    source .env && source .clickhouse_state
    python scripts/02_migrate_trips.py
    python scripts/02_migrate_trips.py --batch-size 50000
    python scripts/02_migrate_trips.py --resume     # skip rows already in ClickHouse

Reads from:  NYC_TAXI_DB.RAW.TRIPS_RAW  (Snowflake)
Writes to:   default.trips_raw           (ClickHouse)

Required environment variables:
    SNOWFLAKE_ORG        e.g. myorg
    SNOWFLAKE_ACCOUNT    e.g. abc12345
    SNOWFLAKE_USER       e.g. MIGRATION_USER
    SNOWFLAKE_PASSWORD
    CLICKHOUSE_HOST      e.g. abc123.us-east-1.aws.clickhouse.cloud
    CLICKHOUSE_PASSWORD
    CLICKHOUSE_USER      (default: default)
    CLICKHOUSE_PORT      (default: 8443)

Install dependencies:
    pip install snowflake-connector-python clickhouse-connect
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
try:
    import snowflake.connector
except ImportError:
    sys.exit("Missing dependency: pip install snowflake-connector-python")

try:
    import clickhouse_connect
except ImportError:
    sys.exit("Missing dependency: pip install clickhouse-connect")

# ---------------------------------------------------------------------------
# Column mapping: Snowflake name → ClickHouse name
# Order must match the SELECT below.
# ---------------------------------------------------------------------------
COLUMN_MAP = [
    ("TRIP_ID",          "trip_id"),
    ("VENDOR_ID",        "vendor_id"),
    ("PICKUP_DATETIME",  "pickup_at"),
    ("DROPOFF_DATETIME", "dropoff_at"),
    ("PASSENGER_COUNT",  "passenger_count"),
    ("TRIP_DISTANCE",    "trip_distance_miles"),
    ("RATECODE_ID",      "rate_code_id"),
    ("STORE_FWD_FLAG",   "store_fwd_flag"),
    ("PU_LOCATION_ID",   "pickup_location_id"),
    ("DO_LOCATION_ID",   "dropoff_location_id"),
    ("PAYMENT_TYPE",     "payment_type_id"),
    ("FARE_AMOUNT",      "fare_amount_usd"),
    ("EXTRA",            "extra_amount_usd"),
    ("MTA_TAX",          "mta_tax_usd"),
    ("TIP_AMOUNT",       "tip_amount_usd"),
    ("TOLLS_AMOUNT",     "tolls_amount_usd"),
    ("TOTAL_AMOUNT",     "total_amount_usd"),
    ("INGESTED_AT",      "ingested_at"),
    ("TRIP_METADATA",    "trip_metadata"),
]

SF_COLUMNS   = [sf  for sf, _ in COLUMN_MAP]
CH_COLUMNS   = [ch  for _, ch in COLUMN_MAP]

# Index of TRIP_METADATA in the row tuple (last column)
METADATA_IDX = SF_COLUMNS.index("TRIP_METADATA")


def get_env(name: str, default: str = None) -> str:
    val = os.environ.get(name, default)
    if val is None:
        sys.exit(f"Error: environment variable {name} is not set. Source .env and .clickhouse_state first.")
    return val


def connect_snowflake() -> snowflake.connector.SnowflakeConnection:
    org     = get_env("SNOWFLAKE_ORG")
    account = get_env("SNOWFLAKE_ACCOUNT")
    user    = get_env("SNOWFLAKE_USER")
    password = get_env("SNOWFLAKE_PASSWORD")
    account_id = f"{org}-{account}"
    print(f"  Connecting to Snowflake: {account_id}")
    conn = snowflake.connector.connect(
        account=account_id,
        user=user,
        password=password,
        warehouse="TRANSFORM_WH",
        database="NYC_TAXI_DB",
        schema="RAW",
    )
    print("  Snowflake: connected")
    return conn


def connect_clickhouse():
    host     = get_env("CLICKHOUSE_HOST")
    password = get_env("CLICKHOUSE_PASSWORD")
    user     = get_env("CLICKHOUSE_USER", "default")
    port     = int(get_env("CLICKHOUSE_PORT", "8443"))
    print(f"  Connecting to ClickHouse: {host}:{port}")
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        secure=True,
    )
    print("  ClickHouse: connected")
    return client


def get_resume_watermark(ch_client) -> datetime | None:
    """Return the max pickup_at already in ClickHouse, or None if table is empty."""
    result = ch_client.query("SELECT max(pickup_at) FROM default.trips_raw")
    val = result.first_row[0]
    if val is None or (hasattr(val, 'year') and val.year == 1970):
        return None
    return val


def build_sf_query(resume_after: datetime | None, batch_size: int) -> str:
    select = ", ".join(SF_COLUMNS)
    base = f"SELECT {select} FROM NYC_TAXI_DB.RAW.TRIPS_RAW"
    if resume_after:
        ts = resume_after.strftime("%Y-%m-%d %H:%M:%S.%f")
        base += f" WHERE PICKUP_DATETIME > '{ts}'"
    base += " ORDER BY PICKUP_DATETIME, TRIP_ID"
    return base


def coerce_row(row: tuple) -> list:
    """Convert Snowflake row values to ClickHouse-compatible types."""
    row = list(row)

    # TRIP_METADATA: Snowflake VARIANT comes back as a dict or already-serialized string.
    meta = row[METADATA_IDX]
    if meta is None:
        row[METADATA_IDX] = ""
    elif isinstance(meta, dict):
        row[METADATA_IDX] = json.dumps(meta)
    else:
        row[METADATA_IDX] = str(meta)

    # STORE_FWD_FLAG: coerce None → ""
    sfwd_idx = SF_COLUMNS.index("STORE_FWD_FLAG")
    if row[sfwd_idx] is None:
        row[sfwd_idx] = ""

    # Numeric NULLs → 0
    numeric_cols = [
        "VENDOR_ID", "PASSENGER_COUNT", "TRIP_DISTANCE", "RATECODE_ID",
        "PU_LOCATION_ID", "DO_LOCATION_ID", "PAYMENT_TYPE",
        "FARE_AMOUNT", "EXTRA", "MTA_TAX", "TIP_AMOUNT", "TOLLS_AMOUNT", "TOTAL_AMOUNT",
    ]
    for col in numeric_cols:
        idx = SF_COLUMNS.index(col)
        if row[idx] is None:
            row[idx] = 0

    return row


def fmt(n: int) -> str:
    return f"{n:,}"


def eta_str(elapsed: float, done: int, total: int) -> str:
    if done == 0:
        return "calculating..."
    rate = done / elapsed
    remaining_rows = total - done
    remaining_secs = remaining_rows / rate
    m, s = divmod(int(remaining_secs), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m remaining"
    elif m > 0:
        return f"{m}m {s}s remaining"
    else:
        return f"{s}s remaining"


def main():
    parser = argparse.ArgumentParser(description="Migrate NYC Taxi trips from Snowflake to ClickHouse")
    parser.add_argument("--batch-size", type=int, default=100_000, help="Rows per ClickHouse INSERT (default: 100000)")
    parser.add_argument("--resume", action="store_true", help="Skip rows already in ClickHouse (uses max pickup_at as watermark)")
    args = parser.parse_args()

    DIVIDER = "━" * 60
    print()
    print(DIVIDER)
    print("  NYC Taxi Migration: Snowflake → ClickHouse")
    print(DIVIDER)
    print()

    # Connect
    sf_conn  = connect_snowflake()
    ch_client = connect_clickhouse()
    print()

    # Resume watermark
    resume_after = None
    if args.resume:
        resume_after = get_resume_watermark(ch_client)
        if resume_after:
            print(f"  Resume mode: skipping rows with pickup_at <= {resume_after}")
        else:
            print("  Resume mode: ClickHouse table is empty, starting from the beginning")
        print()

    # Total row count for progress
    count_query = "SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.TRIPS_RAW"
    if resume_after:
        ts = resume_after.strftime("%Y-%m-%d %H:%M:%S.%f")
        count_query += f" WHERE PICKUP_DATETIME > '{ts}'"
    sf_cur = sf_conn.cursor()
    sf_cur.execute(count_query)
    total_rows = sf_cur.fetchone()[0]
    sf_cur.close()
    print(f"  Rows to migrate: {fmt(total_rows)}")
    print(f"  Batch size:      {fmt(args.batch_size)}")
    print()

    if total_rows == 0:
        print("  Nothing to migrate. Exiting.")
        sf_conn.close()
        ch_client.close()
        return

    # Stream rows from Snowflake in batches
    query = build_sf_query(resume_after, args.batch_size)
    sf_cur = sf_conn.cursor()
    sf_cur.execute(query)

    total_inserted = 0
    batch: list[list] = []
    start_time = time.time()

    print(f"  {'Rows inserted':<20} {'Elapsed':<12} {'ETA':<22} {'Rate'}")
    print(f"  {'-'*20} {'-'*12} {'-'*22} {'-'*15}")

    for raw_row in sf_cur:
        batch.append(coerce_row(raw_row))

        if len(batch) >= args.batch_size:
            ch_client.insert("default.trips_raw", batch, column_names=CH_COLUMNS)
            total_inserted += len(batch)
            batch = []

            elapsed = time.time() - start_time
            rate = total_inserted / elapsed
            eta = eta_str(elapsed, total_inserted, total_rows)
            pct = total_inserted * 100 / total_rows
            print(
                f"  {fmt(total_inserted):<20} "
                f"{int(elapsed//60)}m {int(elapsed%60):02d}s      "
                f"{eta:<22} "
                f"{fmt(int(rate))} rows/s  ({pct:.1f}%)"
            )

    # Flush remainder
    if batch:
        ch_client.insert("default.trips_raw", batch, column_names=CH_COLUMNS)
        total_inserted += len(batch)

    sf_cur.close()
    sf_conn.close()
    ch_client.close()

    elapsed = time.time() - start_time
    rate = total_inserted / elapsed if elapsed > 0 else 0

    print()
    print(DIVIDER)
    print(f"  Migration complete")
    print(f"  Rows inserted: {fmt(total_inserted)}")
    print(f"  Total time:    {int(elapsed//60)}m {int(elapsed%60):02d}s")
    print(f"  Avg rate:      {fmt(int(rate))} rows/s")
    print(DIVIDER)
    print()
    print("  Next step: verify with  bash scripts/01_verify_migration.sh")
    print()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
NYC Taxi Trip Producer
----------------------
Continuously generates realistic fake taxi trips and inserts them into
NYC_TAXI_DB.RAW.TRIPS_RAW, keeping the CDC stream and dashboards live.

Configuration (env vars):
  SNOWFLAKE_ACCOUNT       ORG-ACCOUNT format  (required)
  SNOWFLAKE_USER          Snowflake username   (required)
  SNOWFLAKE_PASSWORD      Password             (required unless using key-pair)
  SNOWFLAKE_PRIVATE_KEY_PATH  Path to .p8 file (alternative to password)
  SNOWFLAKE_ROLE          Default: LOADER_ROLE
  SNOWFLAKE_WAREHOUSE     Default: TRANSFORM_WH
  TRIPS_PER_MINUTE        Default: 60
  BATCH_INTERVAL_SECONDS  Default: 10
  LOG_LEVEL               Default: INFO
"""
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta

import snowflake.connector

# ── Configuration ──────────────────────────────────────────────────────────────

SNOWFLAKE_ACCOUNT       = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER          = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD      = os.environ.get("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_PRIVATE_KEY   = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")
SNOWFLAKE_ROLE          = os.environ.get("SNOWFLAKE_ROLE", "LOADER_ROLE")
SNOWFLAKE_WAREHOUSE     = os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH")
SNOWFLAKE_DATABASE      = "NYC_TAXI_DB"
SNOWFLAKE_SCHEMA        = "RAW"

TRIPS_PER_MINUTE      = float(os.environ.get("TRIPS_PER_MINUTE", "60"))
BATCH_INTERVAL_SECS   = int(os.environ.get("BATCH_INTERVAL_SECONDS", "10"))
TRIPS_PER_BATCH       = max(1, round(TRIPS_PER_MINUTE * BATCH_INTERVAL_SECS / 60))

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [producer] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── Static reference data ──────────────────────────────────────────────────────

# Manhattan zones (roughly 1-103 + some higher IDs) appear 4x more often
_MANHATTAN = (
    list(range(4, 12)) + list(range(13, 45)) + list(range(46, 78)) +
    list(range(79, 104)) + [107, 113, 114, 125, 140, 141, 142, 143, 144, 148,
    151, 152, 153, 158, 161, 162, 163, 164, 166, 170, 186, 194, 202, 209,
    211, 224, 231, 234, 236, 239, 243, 244, 246, 249, 261, 262, 263]
)
_OTHER = [z for z in range(1, 266) if z not in _MANHATTAN]
WEIGHTED_ZONES = _MANHATTAN * 4 + _OTHER

VEHICLE_TYPES   = ["Sedan", "SUV", "Minivan", "Luxury"]
PLATFORMS       = ["iOS", "Android", "Web"]
TRAFFIC_LEVELS  = ["none", "light", "moderate", "heavy"]
TRAFFIC_WEIGHTS = [20, 40, 30, 10]

# Realistic app version pool
APP_VERSIONS = [
    f"{maj}.{minor}.{patch}"
    for maj in range(2, 5)
    for minor in range(0, 8)
    for patch in range(0, 5)
]

# ── Trip generation ────────────────────────────────────────────────────────────

def make_trip() -> tuple:
    """Return one row tuple matching TRIPS_RAW column order."""
    vendor_id       = random.randint(1, 3)
    pu_zone         = random.choice(WEIGHTED_ZONES)
    do_zone         = random.choice(WEIGHTED_ZONES)
    passenger_count = random.choices([1, 2, 3, 4, 5, 6], weights=[55, 20, 10, 8, 5, 2])[0]
    payment_type    = random.choices([1, 2, 3, 4, 5, 6], weights=[65, 30, 1, 2, 1, 1])[0]
    rate_code_id    = random.choices([1, 2, 3, 4, 5, 6], weights=[88, 4, 1, 1, 4, 2])[0]
    store_fwd       = random.choices(["N", "Y"], weights=[98, 2])[0]

    # Distance: exponential distribution gives realistic long-tail (mean ~4 miles)
    distance = round(max(0.1, min(random.expovariate(1 / 4.0), 60.0)), 2)

    # Duration roughly proportional to distance + traffic noise
    traffic_level = random.choices(TRAFFIC_LEVELS, weights=TRAFFIC_WEIGHTS)[0]
    traffic_factor = {"none": 3.5, "light": 4.5, "moderate": 6.0, "heavy": 8.5}[traffic_level]
    duration_min = max(3, min(120, int(distance * traffic_factor + random.gauss(3, 2))))
    estimated_min = max(3, int(distance * 4.5 + random.gauss(2, 1)))

    # Trip ended within the last 90 seconds (just completed)
    dropoff_dt = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=random.randint(0, 90))
    pickup_dt  = dropoff_dt - timedelta(minutes=duration_min)

    # Fare: TLC rate card approximation
    # Standard rate: $3.00 base + $1.75/mile
    fare   = round(3.00 + distance * 1.75, 2)
    extra  = round(random.choices([0.0, 0.5, 1.0], weights=[60, 30, 10])[0], 2)
    mta    = 0.50
    tolls  = round(random.choices([0.0, 6.12, 11.52], weights=[85, 10, 5])[0], 2)
    tip    = round(fare * random.uniform(0.15, 0.25), 2) if payment_type == 1 else 0.0
    total  = round(fare + extra + mta + tip + tolls, 2)

    # Surge: rare high surge, occasional medium, common none
    surge_roll = random.random()
    if surge_roll < 0.03:
        surge = round(random.uniform(2.0, 3.5), 1)
    elif surge_roll < 0.10:
        surge = round(random.uniform(1.5, 2.0), 1)
    elif surge_roll < 0.25:
        surge = round(random.uniform(1.1, 1.5), 1)
    else:
        surge = 1.0

    metadata = json.dumps({
        "driver": {
            "rating":          max(1.0, min(5.0, round(random.gauss(4.6, 0.3), 1))),
            "trips_completed": random.randint(50, 8000),
            "vehicle_type":    random.choice(VEHICLE_TYPES),
        },
        "app": {
            "version":          random.choice(APP_VERSIONS),
            "platform":         random.choice(PLATFORMS),
            "surge_multiplier": surge,
        },
        "route": {
            "estimated_minutes": estimated_min,
            "actual_minutes":    duration_min,
            "traffic_level":     traffic_level,
        },
    })

    return (
        str(uuid.uuid4()),  # TRIP_ID
        vendor_id,          # VENDOR_ID
        pickup_dt,          # PICKUP_DATETIME
        dropoff_dt,         # DROPOFF_DATETIME
        passenger_count,    # PASSENGER_COUNT
        distance,           # TRIP_DISTANCE
        rate_code_id,       # RATECODE_ID
        store_fwd,          # STORE_FWD_FLAG
        pu_zone,            # PU_LOCATION_ID
        do_zone,            # DO_LOCATION_ID
        payment_type,       # PAYMENT_TYPE
        fare,               # FARE_AMOUNT
        extra,              # EXTRA
        mta,                # MTA_TAX
        tip,                # TIP_AMOUNT
        tolls,              # TOLLS_AMOUNT
        total,              # TOTAL_AMOUNT
        metadata,           # TRIP_METADATA → PARSE_JSON
    )


INSERT_SQL = """
INSERT INTO NYC_TAXI_DB.RAW.TRIPS_RAW (
    TRIP_ID, VENDOR_ID, PICKUP_DATETIME, DROPOFF_DATETIME,
    PASSENGER_COUNT, TRIP_DISTANCE, RATECODE_ID, STORE_FWD_FLAG,
    PU_LOCATION_ID, DO_LOCATION_ID, PAYMENT_TYPE,
    FARE_AMOUNT, EXTRA, MTA_TAX, TIP_AMOUNT, TOLLS_AMOUNT, TOTAL_AMOUNT,
    TRIP_METADATA
)
SELECT
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    PARSE_JSON(%s)
"""

# ── Snowflake connection ───────────────────────────────────────────────────────

def _load_private_key(path: str):
    """Load an unencrypted RSA private key (.p8) for key-pair auth."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.serialization import (
        Encoding, PrivateFormat, NoEncryption, load_pem_private_key
    )
    with open(path, "rb") as f:
        private_key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return private_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def connect() -> snowflake.connector.SnowflakeConnection:
    log.info("Connecting  account=%s  user=%s  role=%s  warehouse=%s",
             SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE)

    kwargs = dict(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        role      = SNOWFLAKE_ROLE,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database  = SNOWFLAKE_DATABASE,
        schema    = SNOWFLAKE_SCHEMA,
    )
    if SNOWFLAKE_PRIVATE_KEY:
        kwargs["private_key"] = _load_private_key(SNOWFLAKE_PRIVATE_KEY)
    else:
        kwargs["password"] = SNOWFLAKE_PASSWORD

    conn = snowflake.connector.connect(**kwargs)
    log.info("Connected.")
    return conn

# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    conn   = connect()
    cursor = conn.cursor()
    total  = 0
    start  = time.monotonic()

    log.info("Producer running — %.0f trips/min, %d per batch, interval %ds",
             TRIPS_PER_MINUTE, TRIPS_PER_BATCH, BATCH_INTERVAL_SECS)

    while True:
        batch_start = time.monotonic()
        rows = [make_trip() for _ in range(TRIPS_PER_BATCH)]

        try:
            for row in rows:
                cursor.execute(INSERT_SQL, row)
            total += len(rows)
            elapsed_min = (time.monotonic() - start) / 60 or 0.001
            log.info("✓ inserted %3d trips  |  total=%6d  |  actual rate=%.1f trips/min",
                     len(rows), total, total / elapsed_min)
        except snowflake.connector.errors.DatabaseError as exc:
            log.error("Insert failed: %s — reconnecting in 5s", exc)
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(5)
            conn   = connect()
            cursor = conn.cursor()
            continue

        sleep_secs = max(0.0, BATCH_INTERVAL_SECS - (time.monotonic() - batch_start))
        time.sleep(sleep_secs)


def _shutdown(sig, _frame):
    log.info("Received signal %s — shutting down.", sig)
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    log.info("NYC Taxi Trip Producer starting  "
             "(TRIPS_PER_MINUTE=%.0f  BATCH_INTERVAL=%ds  TRIPS_PER_BATCH=%d)",
             TRIPS_PER_MINUTE, BATCH_INTERVAL_SECS, TRIPS_PER_BATCH)

    # Outer retry loop keeps the container alive through transient Snowflake errors
    backoff = 15
    while True:
        try:
            run()
        except KeyboardInterrupt:
            log.info("Interrupted.")
            sys.exit(0)
        except Exception as exc:
            log.error("Unexpected error: %s — retrying in %ds", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)  # cap at 2 minutes
        else:
            backoff = 15  # reset on clean restart

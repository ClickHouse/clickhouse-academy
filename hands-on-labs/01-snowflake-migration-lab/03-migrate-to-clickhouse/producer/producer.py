#!/usr/bin/env python3
"""
NYC Taxi Trip Producer — ClickHouse
-------------------------------------
Continuously generates realistic fake taxi trips and inserts them into
default.trips_raw in ClickHouse Cloud, keeping the dashboards live after
producer cutover.

Configuration (env vars):
  CLICKHOUSE_HOST         ClickHouse Cloud hostname   (required)
  CLICKHOUSE_PORT         Default: 8443
  CLICKHOUSE_USER         Default: default
  CLICKHOUSE_PASSWORD     ClickHouse password         (required)
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

import clickhouse_connect

# ── Configuration ──────────────────────────────────────────────────────────────

CLICKHOUSE_HOST     = os.environ["CLICKHOUSE_HOST"]
CLICKHOUSE_PORT     = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
CLICKHOUSE_USER     = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]

TRIPS_PER_MINUTE    = float(os.environ.get("TRIPS_PER_MINUTE", "60"))
BATCH_INTERVAL_SECS = int(os.environ.get("BATCH_INTERVAL_SECONDS", "10"))
TRIPS_PER_BATCH     = max(1, round(TRIPS_PER_MINUTE * BATCH_INTERVAL_SECS / 60))

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [ch-producer] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── Static reference data ──────────────────────────────────────────────────────

# Manhattan zones appear 4x more often (matches historical distribution)
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

APP_VERSIONS = [
    f"{maj}.{minor}.{patch}"
    for maj in range(2, 5)
    for minor in range(0, 8)
    for patch in range(0, 5)
]

# ── Column list for default.trips_raw ─────────────────────────────────────────

COLUMNS = [
    "trip_id", "vendor_id", "pickup_at", "dropoff_at",
    "passenger_count", "trip_distance_miles", "rate_code_id", "store_fwd_flag",
    "pickup_location_id", "dropoff_location_id", "payment_type_id",
    "fare_amount_usd", "extra_amount_usd", "mta_tax_usd",
    "tip_amount_usd", "tolls_amount_usd", "total_amount_usd",
    "ingested_at", "trip_metadata",
]

# ── Trip generation ────────────────────────────────────────────────────────────

def make_trip() -> list:
    """Return one row list matching the COLUMNS order above."""
    vendor_id       = random.randint(1, 3)
    pu_zone         = random.choice(WEIGHTED_ZONES)
    do_zone         = random.choice(WEIGHTED_ZONES)
    passenger_count = random.choices([1, 2, 3, 4, 5, 6], weights=[55, 20, 10, 8, 5, 2])[0]
    payment_type    = random.choices([1, 2, 3, 4, 5, 6], weights=[65, 30, 1, 2, 1, 1])[0]
    rate_code_id    = random.choices([1, 2, 3, 4, 5, 6], weights=[88, 4, 1, 1, 4, 2])[0]
    store_fwd       = random.choices(["N", "Y"], weights=[98, 2])[0]

    distance = round(max(0.1, min(random.expovariate(1 / 4.0), 60.0)), 2)

    traffic_level  = random.choices(TRAFFIC_LEVELS, weights=TRAFFIC_WEIGHTS)[0]
    traffic_factor = {"none": 3.5, "light": 4.5, "moderate": 6.0, "heavy": 8.5}[traffic_level]
    duration_min   = max(3, min(120, int(distance * traffic_factor + random.gauss(3, 2))))
    estimated_min  = max(3, int(distance * 4.5 + random.gauss(2, 1)))

    dropoff_dt = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=random.randint(0, 90))
    pickup_dt  = dropoff_dt - timedelta(minutes=duration_min)

    fare  = round(3.00 + distance * 1.75, 2)
    extra = round(random.choices([0.0, 0.5, 1.0], weights=[60, 30, 10])[0], 2)
    mta   = 0.50
    tolls = round(random.choices([0.0, 6.12, 11.52], weights=[85, 10, 5])[0], 2)
    tip   = round(fare * random.uniform(0.15, 0.25), 2) if payment_type == 1 else 0.0
    total = round(fare + extra + mta + tip + tolls, 2)

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

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    return [
        str(uuid.uuid4()),  # trip_id
        vendor_id,          # vendor_id
        pickup_dt,          # pickup_at
        dropoff_dt,         # dropoff_at
        passenger_count,    # passenger_count
        distance,           # trip_distance_miles
        rate_code_id,       # rate_code_id
        store_fwd,          # store_fwd_flag
        pu_zone,            # pickup_location_id
        do_zone,            # dropoff_location_id
        payment_type,       # payment_type_id
        fare,               # fare_amount_usd
        extra,              # extra_amount_usd
        mta,                # mta_tax_usd
        tip,                # tip_amount_usd
        tolls,              # tolls_amount_usd
        total,              # total_amount_usd
        now,                # ingested_at
        metadata,           # trip_metadata
    ]

# ── ClickHouse connection ──────────────────────────────────────────────────────

def connect() -> clickhouse_connect.driver.Client:
    log.info("Connecting  host=%s  port=%d  user=%s", CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER)
    client = clickhouse_connect.get_client(
        host     = CLICKHOUSE_HOST,
        port     = CLICKHOUSE_PORT,
        username = CLICKHOUSE_USER,
        password = CLICKHOUSE_PASSWORD,
        secure   = True,
    )
    # Verify connectivity
    client.command("SELECT 1")
    log.info("Connected.")
    return client

# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    client = connect()
    total  = 0
    start  = time.monotonic()

    log.info("Producer running — %.0f trips/min, %d per batch, interval %ds",
             TRIPS_PER_MINUTE, TRIPS_PER_BATCH, BATCH_INTERVAL_SECS)

    while True:
        batch_start = time.monotonic()
        rows = [make_trip() for _ in range(TRIPS_PER_BATCH)]

        try:
            client.insert("default.trips_raw", rows, column_names=COLUMNS)
            total += len(rows)
            elapsed_min = (time.monotonic() - start) / 60 or 0.001
            log.info("✓ inserted %3d trips  |  total=%6d  |  actual rate=%.1f trips/min",
                     len(rows), total, total / elapsed_min)
        except Exception as exc:
            log.error("Insert failed: %s — reconnecting in 5s", exc)
            time.sleep(5)
            try:
                client = connect()
            except Exception as conn_exc:
                log.error("Reconnect failed: %s — will retry next batch", conn_exc)
            continue

        sleep_secs = max(0.0, BATCH_INTERVAL_SECS - (time.monotonic() - batch_start))
        time.sleep(sleep_secs)


def _shutdown(sig, _frame):
    log.info("Received signal %s — shutting down.", sig)
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    log.info("NYC Taxi Trip Producer (ClickHouse) starting  "
             "(TRIPS_PER_MINUTE=%.0f  BATCH_INTERVAL=%ds  TRIPS_PER_BATCH=%d)",
             TRIPS_PER_MINUTE, BATCH_INTERVAL_SECS, TRIPS_PER_BATCH)

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
            backoff = min(backoff * 2, 120)
        else:
            backoff = 15

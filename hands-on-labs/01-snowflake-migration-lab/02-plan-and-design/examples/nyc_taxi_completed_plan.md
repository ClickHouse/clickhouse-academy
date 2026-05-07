# NYC Taxi — Completed Migration Plan (Worked Example)

This is the fully worked-out answer for all four worksheets applied to the NYC Taxi workload. Use it to:
- Check your worksheet answers after completing each section
- Understand the reasoning behind decisions that Part 3 implements
- Compare against Part 3's Decision Alignment table if you chose differently

**This is the answer key — do not fill it in as your plan. Fill in `migration-plan.md` instead.**

---

## Completion Checklist

- [x] Engine selection: completed
- [x] Sort key design: completed
- [x] Schema translation: completed
- [x] Migration wave plan: completed
- [x] dbt model design: completed

---

## Section 1: Profile Summary

| Metric | Value |
|--------|-------|
| Total tables | 7 (TRIPS_RAW, FACT_TRIPS, AGG_HOURLY_ZONE_TRIPS, DIM_TAXI_ZONES, DIM_PAYMENT_TYPE, DIM_VENDOR, DIM_DATE) |
| Total views | 2 (STG_TRIPS, STG_TAXI_ZONES) |
| Streams | 1 (TRIPS_CDC_STREAM on TRIPS_RAW) |
| Tasks | 2 (CDC_CONSUME_TASK, HOURLY_AGG_TASK) |
| Total rows in TRIPS_RAW | ~50,000,000 |
| Date range | 4-year rolling window ending at setup time |
| VARIANT columns | 1 (TRIPS_RAW.TRIP_METADATA) |
| QUALIFY usages detected | 1 (Q3 query) |
| MERGE INTO usages detected | 2 (HOURLY_AGG_TASK, CDC_CONSUME_TASK) |

---

## Section 2: Object Inventory

| Object | Type | Schema | Rows | Complexity Grade | Notes |
|--------|------|--------|------|-----------------|-------|
| `trips_raw` | Table | raw | ~50M | B | RMT with `_synced_at` version column; bulk + CDC overlap requires dedup; `stg_trips` must use FINAL |
| `stg_trips` | dbt View | staging | — | B | JSONExtract for TRIP_METADATA; requires JSON path testing |
| `stg_taxi_zones` | dbt View | staging | — | A | Passthrough; trivial |
| `int_trips_enriched` | dbt Ephemeral | staging | — | A | CTE; SQL differences handled in parent models |
| `fact_trips` | dbt Incremental | analytics | ~50M | C | RMT engine; delete_insert; QUALIFY rewrite; FINAL required |
| `agg_hourly_zone_trips` | dbt Incremental | analytics | ~140K | B | RMT; rolling 2-hr recalc window; test partition boundary carefully |
| `dim_taxi_zones` | dbt Table | analytics | 265 | A | Static reference; full reload; trivial |
| `dim_payment_type` | dbt Table | analytics | 6 | A | Static reference; trivial |
| `dim_vendor` | dbt Table | analytics | 3 | A | Static reference; trivial |
| `taxi_zones_dict` | Dictionary | analytics | 265 | B | ClickHouse-specific syntax; dictGet() at query time |
| `mv_hourly_revenue` | Refreshable MV | analytics | — | B | REFRESH EVERY syntax; verify atomic replacement |
| `TRIPS_CDC_STREAM` / `CDC_CONSUME_TASK` | Snowflake Stream + Task | — | — | D | No ClickHouse equivalent; replaced by direct producer cutover in Part 3 |

---

## Section 3: Engine Selection Decisions

| Table | Engine | Version Column | Reasoning |
|-------|--------|---------------|-----------|
| `trips_raw` | `ReplacingMergeTree(_synced_at)` | `_synced_at` | The Python migration script (`scripts/02_migrate_trips.py`) may retry a batch and re-insert the same `trip_id`. After cutover, the live producer may also retry on transient failures. `_synced_at DateTime DEFAULT now()` is set automatically on INSERT — a later retry has a higher timestamp, so RMT keeps the most recent write. `stg_trips` queries with `FINAL` to enforce dedup before any downstream model runs. |
| `fact_trips` | `ReplacingMergeTree(updated_at)` | `updated_at` | Trips can be corrected (fare adjustments, status changes). Same trip_id re-inserted with updated values. `updated_at` increases monotonically on each correction — higher value wins during RMT dedup. Always query with FINAL. |
| `agg_hourly_zone_trips` | `ReplacingMergeTree(updated_at)` | `updated_at` | dbt recalculates the last 2 hours and re-inserts. Without RMT, old and new aggregates accumulate and double-count. `updated_at` set to `now()` on each dbt run ensures latest values win. |
| `dim_taxi_zones` | `MergeTree()` | — | Full reload by dbt (atomic table swap (full rebuild)). No duplicates can accumulate. No dedup needed. |
| `dim_payment_type` | `MergeTree()` | — | Same — full reload. |
| `dim_vendor` | `MergeTree()` | — | Same — full reload. |
| `mv_hourly_revenue` | `MergeTree()` | — | REFRESHABLE MV replaces its entire result set atomically on each REFRESH. No upserts. |

---

## Section 4: Sort Key Design

| Table | ORDER BY | Reasoning |
|-------|----------|-----------|
| `trips_raw` | `(pickup_at, trip_id)` | Time-range scans filter on pickup_at first. trip_id is the RMT dedup key — it must be in ORDER BY so RMT can identify which rows are duplicates. pickup_at first because analytical range scans dominate; trip_id last because it is high-cardinality and acts only as the uniqueness discriminator. |
| `fact_trips` | `(toStartOfMonth(pickup_at), pickup_at, trip_id)` | All 7 analytical queries filter on pickup_at. Month prefix clusters calendar-month data into adjacent blocks — enables coarse block skipping for monthly aggregations without adding PARTITION BY. trip_id last for RMT uniqueness without disrupting block skipping. |
| `agg_hourly_zone_trips` | `(hour_bucket, zone_id)` | Q6 (and all aggregation queries) filter on hour_bucket and zone_id. hour_bucket has ~35K distinct values; zone_id has 265. hour_bucket first because time-range scans are the primary access pattern. zone_id second for secondary filtering. |
| `dim_taxi_zones` | `(location_id)` | 265 rows = one granule. ORDER BY is irrelevant for performance. location_id as the join key is conventional and aids readability. |

---

## Section 5: Schema Translation Notes

| Column | Snowflake Type | ClickHouse Type | Decision Rationale |
|--------|---------------|----------------|-------------------|
| `TRIP_METADATA` | VARIANT | `String` | Preserves raw JSON exactly. JSONExtract* handles arbitrary paths at query time. Map(String,String) loses nested structures; Tuple requires fixed schema. String is the safe choice for arbitrary JSON. |
| `PICKUP_DATETIME` / `PICKUP_AT` | TIMESTAMP_NTZ(9) | `DateTime64(3, 'UTC')` | Millisecond precision is sufficient for trip timestamps. Nanosecond (`9`) is overkill. 'UTC' makes timezone explicit and avoids DST-related surprises in time-range aggregations. |
| `PICKUP_LOCATION_ID` | INTEGER | `UInt16` | Values 1–265. UInt8 max is 255 (too small). UInt16 max is 65535 (correct). 2 bytes vs 4 for Int32 — saves ~95MB uncompressed across 50M rows per column. |
| `VENDOR_ID` | INTEGER | `UInt8` | Values 1–3. UInt8 max is 255 — correct. 1 byte per row. |
| `DRIVER_RATING` | FLOAT | `Nullable(Float32)` | Frequently NULL (not all trips have ratings). Nullable preserves correct null semantics. Float32 is sufficient for 1.0–5.0 range. Float64 would waste storage without adding meaningful precision. |
| `UPDATED_AT` | TIMESTAMP_NTZ(9) | `DateTime64(3, 'UTC')` | Version column for ReplacingMergeTree. Must use DateTime64 not DateTime — two corrections in the same second would be non-deterministic with second precision. Millisecond precision ensures correct dedup ordering. |

### Function Translations Required

| Snowflake Expression | ClickHouse Equivalent |
|----------------------|-----------------------|
| `DATE_TRUNC('hour', pickup_at)` | `toStartOfHour(pickup_at)` |
| `DATEADD('day', -7, CURRENT_DATE)` | `today() - 7` |
| `DATEDIFF('minute', pickup_at, dropoff_at)` | `dateDiff('minute', pickup_at, dropoff_at)` |
| `TRIP_METADATA:driver.rating::FLOAT` | `JSONExtractFloat(trip_metadata, 'driver', 'rating')` |
| `TRIP_METADATA:surge_multiplier::FLOAT` | `JSONExtractFloat(trip_metadata, 'surge_multiplier')` |
| `QUALIFY ROW_NUMBER() OVER (PARTITION BY pickup_location_id ORDER BY fare_amount DESC) <= 10` | `SELECT ... FROM (SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM ...) WHERE rn <= 10` |
| `MERGE INTO fact_trips ... WHEN MATCHED THEN UPDATE` | dbt `delete_insert` incremental — DELETE rows with matching keys, then INSERT all new rows |

---

## Section 6: Migration Waves

| Wave | Objects | Dependencies | Notes |
|------|---------|-------------|-------|
| Wave 0 | `trips_raw` (schema), `dim_taxi_zones`, `dim_payment_type`, `dim_vendor` | None | dbt creates empty tables. Dim tables populated from static reference data immediately (no dependency on trips). Run: `dbt run --select trips_raw dim_*` |
| Wave 1 | Python bulk load (`scripts/02_migrate_trips.py`) | Wave 0 (`trips_raw` schema must exist) | 50M rows from Snowflake TRIPS_RAW. Resumable with `--resume`. Verify row count with `scripts/01_verify_migration.sh`. ~40-50 min. |
| Wave 2 | `stg_trips`, `stg_taxi_zones`, `int_trips_enriched`, `fact_trips`, `agg_hourly_zone_trips` | Wave 1 complete (trips_raw populated) + Wave 0 (dim tables exist) | Full `dbt run`. `stg_trips` reads trips_raw; `int_trips_enriched` joins with dims; `fact_trips` and `agg_hourly_zone_trips` build on top. |
| Wave 3 | `taxi_zones_dict`, `mv_hourly_revenue` | Wave 2 (`dim_taxi_zones` populated for dict; `fact_trips` populated for MV) | Dictionary created via `scripts/04_create_dictionary.sql`. Refreshable MV created via dbt model; first REFRESH runs automatically. |
| Wave 4 | Producer cutover (`scripts/03_cutover.sh`) | Wave 1 complete (bulk load verified) + Wave 2 complete (analytics layer built) | Stop Snowflake producer; start ClickHouse producer writing directly to ClickHouse Cloud; run `dbt run` to populate agg_hourly_zone_trips with live data. |

### Risk Register (Grade C/D objects)

| Object | Risk | Verification Method |
|--------|------|---------------------|
| `fact_trips` | Queries without FINAL over-count during merge lag. `delete_insert` partition range must be scoped to ORDER BY prefix to avoid deleting non-target partitions. | `SELECT COUNT(*) FINAL` matches Snowflake ± CDC lag. Run `dbt test`. Compare Q3 results between systems. Check for duplicate trip_ids: `SELECT trip_id, count() FROM fact_trips GROUP BY trip_id HAVING count() > 1 LIMIT 10`. |
| `agg_hourly_zone_trips` | Rolling 2-hr recalc window must correctly bound the delete range. If too broad, old aggregates deleted; if too narrow, stale aggregates persist. | Spot-check specific `(hour_bucket, zone_id)` tuples against Snowflake. Verify total trip_count across all zones matches Snowflake AGG_HOURLY_ZONE_TRIPS for the same period. |
| Producer cutover | Migration script interrupted mid-run leaves a row-count gap; re-run with `--resume` to fill it. Producer retry after cutover may re-insert trips already in ClickHouse. | `scripts/01_verify_migration.sh` — checks row-count parity between Snowflake and ClickHouse. `ReplacingMergeTree(_synced_at)` handles duplicate inserts idempotently. |

---

## Section 7: Known Dialect Gaps

- [x] QUALIFY — affects: Q3 (`queries/q03_top_trips_qualify.sql`)
- [x] VARIANT colon-path — affects: Q4, Q5 (TRIP_METADATA JSON access)
- [ ] LATERAL FLATTEN — not used in this workload; VARIANT is accessed via colon-path, not FLATTEN
- [x] MERGE INTO — affects: dbt incremental models (fact_trips, agg_hourly_zone_trips)
- [x] Snowflake Streams → producer cutover (live writes go directly to ClickHouse post-cutover)
- [x] Date function differences — affects: Q1 (DATE_TRUNC), Q3 (DATEADD), Q4 (DATEDIFF)

---

## Section 8: Migration Strategy

**Data movement:** Python migration script (`scripts/02_migrate_trips.py`)

Why Python script over object storage relay or ClickPipes?
- `remoteSecure()` is for ClickHouse-to-ClickHouse data transfer — not applicable here.
- Object storage relay (Snowflake → S3 → ClickHouse S3 table function) would work but adds complexity: requires S3 bucket provisioning, IAM roles, and Snowflake COPY INTO — unnecessary overhead for a lab.
- ClickPipes does not support Snowflake as a source. Its supported sources are Kafka, S3, Kinesis, PostgreSQL CDC, and MySQL CDC.
- The Python script uses `snowflake-connector-python` and `clickhouse-connect` — packages already installed for the lab. It shows progress in real time, supports `--resume` for interruptions, and the code is fully inspectable.

**Incremental strategy (dbt):** `delete_insert`

Why `delete_insert` over `append` or `merge` strategy?
- `append` inserts new rows without touching existing ones. For `fact_trips` where rows can be updated, this creates duplicates. Incorrect.
- `merge` (if available) would be the closest to Snowflake's MERGE INTO, but dbt-clickhouse's merge strategy has limitations with ReplacingMergeTree and is not the recommended approach.
- `delete_insert` deletes rows in the incoming batch's key range, then inserts all new rows. This is idempotent (re-running produces the same result), handles both inserts and updates, and works correctly with ReplacingMergeTree. It is the dbt-clickhouse community's standard recommendation for upsert patterns.

---

## Section 9: Cutover Criteria

| Criterion | Threshold | Measured By |
|-----------|-----------|-------------|
| Row count parity | ≥ 99.9% match (CH ≥ SF post-cutover is expected) | `scripts/01_verify_migration.sh` |
| Checksum parity | MD5 match on 10K-row sample | `scripts/02_validate_parity.sql` |
| dbt test pass rate | 100% | `dbt test` in `dbt/nyc_taxi_dbt_ch` |
| Query result parity | All 7 queries return same results (within floating-point tolerance) | Manual comparison in `scripts/run_benchmark.sh` output |

---

---

## Section 10: dbt Model Design

### Materialization Selection

| Model | Materialization | Why |
|-------|-----------------|-----|
| `stg_trips` | `view` | Reads and cleans `trips_raw`; no updates to this model; zero storage cost; always reflects current source state |
| `stg_taxi_zones` | `view` | Same — passthrough cleaning of a source table |
| `int_trips_enriched` | `ephemeral` | Pure join logic used only by `fact_trips`; inlined as a CTE avoids a redundant physical table; no model queries it directly |
| `fact_trips` | `incremental` | Trips can be corrected after the fact; only new and updated rows should be processed per run |
| `agg_hourly_zone_trips` | `incremental` | Rolling 2-hour recalculation is an incremental pattern — process recent rows, not all 50M |
| `dim_taxi_zones` | `table` | 265 static zones; full rebuild on every dbt run via atomic table swap (full rebuild); no partial updates |
| `dim_payment_type` | `table` | 6 static types; same reasoning as dim_taxi_zones |
| `dim_vendor` | `table` | 3 vendors; same reasoning |

### Engine Configuration

| Model | ENGINE | Version Column | Why |
|-------|--------|----------------|-----|
| `fact_trips` | `ReplacingMergeTree(updated_at)` | `updated_at` | Trips can be corrected; `updated_at` set to `now()` on each insert means the latest version wins during RMT background dedup; `delete_insert` is the primary correctness path, RMT is the safety net |
| `agg_hourly_zone_trips` | `ReplacingMergeTree(updated_at)` | `updated_at` | Rolling recalculation re-inserts aggregates for the same `(hour_bucket, zone_id)` pairs; RMT ensures stale aggregates are removed on background merge |
| `dim_taxi_zones` | `MergeTree()` | — | Full reload by dbt means atomic table swap (full rebuild) on every run; duplicates cannot accumulate; no dedup needed |
| `dim_payment_type` | `MergeTree()` | — | Same as dim_taxi_zones |
| `dim_vendor` | `MergeTree()` | — | Same as dim_taxi_zones |

### Incremental Strategy

| Model | `unique_key` | `incremental_strategy` | Incremental filter | Why this filter? |
|-------|-------------|----------------------|-------------------|-----------------|
| `fact_trips` | `trip_id` | `delete_insert` | `WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})` | High-watermark on `updated_at` captures both new trips and corrected trips (fare adjustments re-insert the same `trip_id` with the same `pickup_at` but a newer `updated_at`); a `pickup_at` watermark would silently miss corrections |
| `agg_hourly_zone_trips` | `[hour_bucket, zone_id]` | `delete_insert` | `WHERE pickup_at >= now() - INTERVAL 2 HOUR` | Rolling 2-hour window forces re-aggregation of boundary hours so partial-hour counts are always corrected; a `max(pickup_at)` high-watermark would permanently undercount the boundary hour |

### FINAL Placement

| Model | FINAL in FROM clause? | Why |
|-------|----------------------|-----|
| `stg_trips` | **Yes** — `FROM trips_raw FINAL` | `trips_raw` is ReplacingMergeTree; it can have duplicate `trip_id` rows from migration script retries or post-cutover producer retries. `stg_trips` is the single enforcement point: deduplicate here so every downstream model (int_trips_enriched, fact_trips, agg_hourly_zone_trips) receives clean data |
| `int_trips_enriched` | **No** | Reads from `stg_trips` (a view), not an RMT table; FINAL is irrelevant for views |
| `fact_trips` | **No** (in the model body) | `delete_insert` keeps `fact_trips` clean after each completed run; adding FINAL inside the model would wastefully apply it to the `is_incremental()` subquery reading `max(updated_at)` from `{{ this }}`. Dashboards and dbt tests use `FINAL` externally when querying `fact_trips` directly |

---

*This is the completed example. Your `migration-plan.md` should match the key decisions here — or document explicitly why you chose differently.*

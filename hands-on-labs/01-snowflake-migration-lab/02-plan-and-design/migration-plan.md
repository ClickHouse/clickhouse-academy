# Migration Plan — NYC Taxi Workload

**Partner:** _your name_
**Date:** _today's date_
**Source:** Snowflake — NYC_TAXI_DB
**Target:** ClickHouse Cloud

---

## Completion Checklist

Part 3's `setup.sh` reads these checkboxes. Change `[ ]` to `[x]` when each section is complete.

- [ ] Engine selection: completed
- [ ] Sort key design: completed
- [ ] Schema translation: completed
- [ ] Migration wave plan: completed
- [ ] dbt model design: completed

---

## Section 1: Profile Summary

*Paste key numbers from `profile_report.md` here after running `scripts/01_profile_snowflake.sh`.*

| Metric | Value |
|--------|-------|
| Total tables | |
| Total views | |
| Streams | |
| Tasks | |
| Total rows in TRIPS_RAW | |
| Date range | |
| VARIANT columns | |
| QUALIFY usages detected | |
| MERGE INTO usages detected | |

---

## Section 2: Object Inventory

*List every object being migrated with its complexity grade (A/B/C/D).*

| Object | Type | Schema | Rows | Complexity Grade | Notes |
|--------|------|--------|------|-----------------|-------|
| `trips_raw` | Table | raw | ~50M | | |
| `stg_trips` | dbt View | staging | — | | |
| `stg_taxi_zones` | dbt View | staging | — | | |
| `int_trips_enriched` | dbt Ephemeral | staging | — | | |
| `fact_trips` | dbt Incremental | analytics | ~50M | | |
| `agg_hourly_zone_trips` | dbt Incremental | analytics | | | |
| `dim_taxi_zones` | dbt Table | analytics | 265 | | |
| `dim_payment_type` | dbt Table | analytics | 6 | | |
| `dim_vendor` | dbt Table | analytics | 3 | | |
| `taxi_zones_dict` | Dictionary | analytics | 265 | | |
| `mv_hourly_revenue` | Refreshable MV | analytics | — | | |
| `TRIPS_CDC_STREAM` / `CDC_CONSUME_TASK` | Snowflake Stream + Task | — | — | | |

---

## Section 3: Engine Selection Decisions

*Completed from Worksheet 1.*

| Table | Engine | Version Column | Reasoning |
|-------|--------|---------------|-----------|
| `trips_raw` | | | |
| `fact_trips` | | | |
| `agg_hourly_zone_trips` | | | |
| `dim_taxi_zones` | | — | |
| `dim_payment_type` | | — | |
| `dim_vendor` | | — | |
| `mv_hourly_revenue` | | — | |

---

## Section 4: Sort Key Design

*Completed from Worksheet 2.*

| Table | ORDER BY | Reasoning |
|-------|----------|-----------|
| `trips_raw` | | |
| `fact_trips` | | |
| `agg_hourly_zone_trips` | | |
| `dim_taxi_zones` | | |

---

## Section 5: Schema Translation Notes

*Completed from Worksheet 3. Record only non-obvious decisions.*

| Column | Snowflake Type | ClickHouse Type | Decision Rationale |
|--------|---------------|----------------|-------------------|
| `TRIP_METADATA` | VARIANT | | |
| `PICKUP_DATETIME` | TIMESTAMP_NTZ(9) | | |
| `PICKUP_LOCATION_ID` | INTEGER | | |
| `VENDOR_ID` | INTEGER | | |
| `DRIVER_RATING` | FLOAT | | |
| `UPDATED_AT` | TIMESTAMP_NTZ(9) | | |

### Function Translations Required

| Snowflake Expression | ClickHouse Equivalent |
|----------------------|-----------------------|
| `DATE_TRUNC('hour', ...)` | |
| `DATEADD('day', -7, CURRENT_DATE)` | |
| `DATEDIFF('minute', t1, t2)` | |
| `TRIP_METADATA:driver.rating::FLOAT` | |
| `QUALIFY ROW_NUMBER() OVER (...) <= n` | |
| `MERGE INTO ... WHEN MATCHED THEN UPDATE` | |

---

## Section 6: Migration Waves

*Completed from Worksheet 4.*

| Wave | Objects | Dependencies | Notes |
|------|---------|-------------|-------|
| Wave 0 | | | |
| Wave 1 | | | |
| Wave 2 | | | |
| Wave 3 | | | |
| Wave 4 | | | |

### Risk Register (Grade C/D objects)

| Object | Risk | Verification Method |
|--------|------|---------------------|
| | | |

---

## Section 7: Known Dialect Gaps

*Check all that apply to this workload.*

- [ ] QUALIFY — affects: _list queries_
- [ ] VARIANT colon-path — affects: _list queries_
- [ ] LATERAL FLATTEN — affects: _list queries_
- [ ] MERGE INTO — affects: _list dbt models_
- [ ] Snowflake Streams / Tasks → producer cutover + Refreshable MVs
- [ ] Date function differences — affects: _list queries_

For each checked item, confirm the ClickHouse equivalent is documented in Section 5.

---

## Section 8: Migration Strategy

*Pre-selected for this lab. Annotate your understanding of why.*

**Data movement:** Python migration script (`scripts/02_migrate_trips.py`)
- Bulk load for initial 50M rows via direct Snowflake → ClickHouse connection
- Resumable with `--resume` flag if interrupted
- Post-migration: producer cutover (`scripts/03_cutover.sh`) switches live writes directly to ClickHouse Cloud

Why Python script over object storage relay or ClickPipes?

*Your answer:*

**Incremental strategy (dbt):** `delete_insert`

Why `delete_insert` over `append` or `merge` strategy?

*Your answer:*

---

## Section 9: Cutover Criteria

*These are the minimum requirements before declaring the migration complete. Pre-filled; verify you understand each threshold.*

| Criterion | Threshold | Measured By |
|-----------|-----------|-------------|
| Row count parity | ≥ 99.9% match (CH ≥ SF post-cutover is expected) | `scripts/01_verify_migration.sh` |
| Checksum parity | MD5 match on 10K-row sample | `scripts/02_validate_parity.sql` |
| dbt test pass rate | 100% | `dbt test` in `dbt/nyc_taxi_dbt_ch` |
| Query result parity | All 7 queries return same results (within floating-point tolerance) | Manual comparison |

---

---

## Section 10: dbt Model Design

*Completed from Worksheet 5.*

### Materialization Selection

| Model | Materialization | Why |
|-------|-----------------|-----|
| `stg_trips` | | |
| `stg_taxi_zones` | | |
| `int_trips_enriched` | | |
| `fact_trips` | | |
| `agg_hourly_zone_trips` | | |
| `dim_taxi_zones` | | |
| `dim_payment_type` | | |
| `dim_vendor` | | |

### Engine Configuration

| Model | ENGINE | Version Column | Why |
|-------|--------|----------------|-----|
| `fact_trips` | | | |
| `agg_hourly_zone_trips` | | | |
| `dim_taxi_zones` | | — | |
| `dim_payment_type` | | — | |
| `dim_vendor` | | — | |

### Incremental Strategy

| Model | `unique_key` | `incremental_strategy` | Incremental filter | Why this filter? |
|-------|-------------|----------------------|-------------------|-----------------|
| `fact_trips` | | | | |
| `agg_hourly_zone_trips` | | | | |

### FINAL Placement

| Model | FINAL in FROM clause? | Why? |
|-------|----------------------|------|
| `stg_trips` | | |
| `int_trips_enriched` | | |
| `fact_trips` | | |

---

*When all five checkboxes at the top are checked, proceed to `03-migrate-to-clickhouse/`.*

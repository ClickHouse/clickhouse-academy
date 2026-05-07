# Worksheet 4: Migration Wave Plan

**Time estimate:** 15–20 minutes
**Reference:** [Part 3 README](../../03-migrate-to-clickhouse/README.md) — Section 7 for execution order

---

## Concept

Database objects have dependencies. A view that queries `fact_trips` cannot be created before `fact_trips` exists. A materialized view that reads from `trips_raw` cannot be populated before `trips_raw` has data. Migrating in the wrong order causes create-table failures, empty results, or incomplete data that is hard to diagnose.

**The solution: wave planning.** Organize every object into a numbered wave where each wave contains only objects whose dependencies are satisfied by prior waves.

**Complexity grading** helps prioritize migration effort:
- **Grade A** — trivial: standard table, no special logic, straightforward type mapping
- **Grade B** — medium: dbt incremental model with ClickHouse-specific engine; requires testing
- **Grade C** — complex: requires SQL rewrite (QUALIFY, MERGE INTO, VARIANT); test carefully
- **Grade D** — requires redesign: Snowflake-specific feature with no direct equivalent (Streams → producer cutover to ClickHouse, Tasks → Refreshable MVs or scheduled dbt runs)

---

## Exercise 1: Dependency DAG

Map the dependencies between all objects in the NYC Taxi workload. Draw a line from each object to the objects it depends on.

```
Objects to map:
  Source (Snowflake):
    - TRIPS_RAW (raw table)
    - TRIPS_CDC_STREAM (stream on TRIPS_RAW)
    - CDC_CONSUME_TASK (task reading TRIPS_CDC_STREAM)
    - HOURLY_AGG_TASK (task writing AGG_HOURLY_ZONE_TRIPS)

  ClickHouse targets (via dbt or Python migration script):
    - trips_raw (ClickHouse base table)
    - stg_trips (dbt view)
    - stg_taxi_zones (dbt view)
    - int_trips_enriched (dbt ephemeral CTE)
    - fact_trips (dbt incremental)
    - agg_hourly_zone_trips (dbt incremental)
    - dim_taxi_zones (dbt table)
    - dim_payment_type (dbt table)
    - dim_vendor (dbt table)
    - taxi_zones_dict (ClickHouse dictionary)
    - mv_hourly_revenue (ClickHouse Refreshable MV)
```

Fill in the dependency table:

| Object | Depends On | Notes |
|--------|------------|-------|
| `trips_raw` (ClickHouse) | *(nothing — first object created)* | Schema created by dbt before the Python migration script loads data |
| `stg_trips` | ? | |
| `stg_taxi_zones` | ? | |
| `int_trips_enriched` | ? | |
| `fact_trips` | ? | |
| `agg_hourly_zone_trips` | ? | |
| `dim_taxi_zones` | ? | |
| `dim_payment_type` | ? | *Source data is static reference data — does it depend on trips_raw?* |
| `dim_vendor` | ? | |
| `taxi_zones_dict` | ? | *Dictionary reads from dim_taxi_zones at creation time* |
| `mv_hourly_revenue` | ? | *Refreshable MV queries fact_trips on each refresh* |

---

## Exercise 2: Migration Wave Assignment

Using the dependency map above, assign each object to a migration wave. Objects in the same wave can be created/loaded in parallel. Objects in later waves must wait for earlier waves to complete.

Fill in the wave assignment:

| Wave | Objects | What happens |
|------|---------|--------------|
| Wave 0 | `trips_raw` (schema only), `dim_*` tables | ? |
| Wave 1 | Python bulk load (`scripts/02_migrate_trips.py`), `stg_trips`, `stg_taxi_zones` | ? |
| Wave 2 | ? | ? |
| Wave 3 | ? | ? |
| Wave 4 | ? | ? |

**Constraint:** The Python bulk load (`scripts/02_migrate_trips.py`) must complete before Wave 2 dbt models run — `stg_trips`, `int_trips_enriched`, and `fact_trips` all read from `trips_raw`, which must have data.

---

## Exercise 3: Complexity Grading

Grade each object A/B/C/D and explain the primary migration challenge:

| Object | Grade | Primary Challenge |
|--------|-------|------------------|
| `trips_raw` | ? | |
| `stg_trips` | ? | *Contains JSONExtract for TRIP_METADATA — straightforward but requires JSON path knowledge* |
| `stg_taxi_zones` | ? | |
| `int_trips_enriched` | ? | |
| `fact_trips` | ? | *ReplacingMergeTree, delete_insert incremental, QUALIFY rewrite in Q3* |
| `agg_hourly_zone_trips` | ? | |
| `dim_taxi_zones` | ? | |
| `dim_payment_type` | ? | |
| `dim_vendor` | ? | |
| `taxi_zones_dict` | ? | *ClickHouse-specific syntax; no Snowflake equivalent* |
| `mv_hourly_revenue` | ? | *Refreshable MV syntax is ClickHouse-specific; REFRESH EVERY syntax* |
| `TRIPS_CDC_STREAM` / `CDC_CONSUME_TASK` | ? | *Snowflake-specific; replaced by direct producer cutover to ClickHouse in Part 3* |

---

## Exercise 4: Risk Register

For each Grade C/D object, identify:
1. What can go wrong
2. How you will verify correctness after migration

| Object | Risk | Verification |
|--------|------|--------------|
| `fact_trips` | ? | |
| `agg_hourly_zone_trips` | ? | |
| `TRIPS_CDC_STREAM` / `CDC_CONSUME_TASK` | ? | |

---

<details>
<summary>▶ Answer Key — try the exercises first before expanding</summary>

### Exercise 1: Dependency Table

| Object | Depends On |
|--------|------------|
| `trips_raw` | Nothing (created first; schema before data) |
| `stg_trips` | `trips_raw` (reads from base table) |
| `stg_taxi_zones` | raw source `taxi_zones` — seeded by `scripts/00_seed_zones.sql` into `default.taxi_zones` (distinct from the analytics `dim_taxi_zones` model) |
| `int_trips_enriched` | `stg_trips`, `stg_taxi_zones` (joins both staging views) |
| `fact_trips` | `int_trips_enriched` (dbt CTE chain) |
| `agg_hourly_zone_trips` | `int_trips_enriched` (dbt CTE chain) |
| `dim_taxi_zones` | Reference data (static CSV seed — no CH dependency) |
| `dim_payment_type` | Reference data (static — no dependency) |
| `dim_vendor` | Reference data (static — no dependency) |
| `taxi_zones_dict` | `dim_taxi_zones` (reads from it at dictionary creation time) |
| `mv_hourly_revenue` | `fact_trips` (refreshable MV queries fact_trips) |

### Exercise 2: Migration Wave Assignment

| Wave | Objects | What happens |
|------|---------|--------------|
| Wave 0 | `trips_raw` schema, `dim_taxi_zones`, `dim_payment_type`, `dim_vendor` | dbt creates empty tables; reference dims populated immediately (static data) |
| Wave 1 | Python bulk load (`scripts/02_migrate_trips.py`) → `trips_raw` | 50M rows loaded from Snowflake; verify row count parity with `scripts/01_verify_migration.sh` |
| Wave 2 | `stg_trips`, `stg_taxi_zones`, `int_trips_enriched` (ephemeral), `fact_trips`, `agg_hourly_zone_trips` | `dbt run` builds the analytics layer from loaded data |
| Wave 3 | `taxi_zones_dict`, `mv_hourly_revenue` | Dictionary created after `dim_taxi_zones` is populated; refreshable MV created after `fact_trips` has data |
| Wave 4 | Producer cutover (`scripts/03_cutover.sh`) | Stop Snowflake producer; run final migration delta; start ClickHouse producer writing directly to ClickHouse Cloud |

### Exercise 3: Complexity Grading

| Object | Grade | Challenge |
|--------|-------|-----------|
| `trips_raw` | A | ReplacingMergeTree(_synced_at); migration retries and post-cutover producer retries require idempotent inserts — same decision as Worksheet 1 |
| `stg_trips` | B | JSONExtract for TRIP_METADATA; requires JSON path knowledge and testing |
| `stg_taxi_zones` | A | Passthrough view; trivial translation |
| `int_trips_enriched` | A | Ephemeral CTE; SQL differences are handled in parent models |
| `fact_trips` | C | ReplacingMergeTree engine; `delete_insert` incremental strategy; QUALIFY treated as a dialect gap in Q3 (subquery rewrite); must use FINAL in queries |
| `agg_hourly_zone_trips` | B | ReplacingMergeTree; rolling window recalculation logic; `delete_insert` strategy with 2-hr window |
| `dim_taxi_zones` | A | Static reference; full reload; trivial |
| `dim_payment_type` | A | Static reference; trivial |
| `dim_vendor` | A | Static reference; trivial |
| `taxi_zones_dict` | B | ClickHouse-specific `CREATE DICTIONARY` syntax; `HASHED()` layout; dictGet() at query time |
| `mv_hourly_revenue` | B | `REFRESH EVERY 180 SECOND` syntax; must verify it replaces data atomically |
| `TRIPS_CDC_STREAM` / `CDC_CONSUME_TASK` | D | Snowflake Streams and Tasks have no direct ClickHouse equivalent; replaced by direct producer cutover in Part 3 — the live trip producer writes to ClickHouse Cloud after cutover |

### Exercise 4: Risk Register

| Object | Risk | Verification |
|--------|------|--------------|
| `fact_trips` | Queries without FINAL over-count rows during merge lag period; `delete_insert` partition range must match ORDER BY prefix to avoid deleting non-target rows | `SELECT COUNT(*) FROM fact_trips FINAL` matches Snowflake `COUNT(*)` within CDC lag tolerance; run `dbt test` and compare Q3 output between systems |
| `agg_hourly_zone_trips` | Rolling 2-hr recalculation window in dbt must correctly scope the delete range to avoid deleting hours outside the recalc window | Spot-check specific `(hour_bucket, zone_id)` pairs against Snowflake; verify no older hours disappear after a dbt run |
| `TRIPS_CDC_STREAM` / producer cutover | Migration script interrupted mid-run leaves a row-count gap; producer retry after cutover may re-insert trips already in ClickHouse | Run `scripts/01_verify_migration.sh` for row-count parity; `ReplacingMergeTree(_synced_at)` makes producer retries idempotent |

</details>

---

## Transfer to migration-plan.md

Copy your wave plan and complexity grades to Section 6 of `migration-plan.md` and check off:

```
- [ ] Migration wave plan: completed
```

# Worksheet 5: dbt Model Design

**Time estimate:** 15–20 minutes
**Reference:** [`docs/dbt_clickhouse_guide.md`](../docs/dbt_clickhouse_guide.md)

---

## Concept

Worksheets 1–4 produced the *what*: which engine, which ORDER BY, which ClickHouse types. This worksheet produces the *how*: how those decisions are expressed as dbt configuration before writing any SQL.

A dbt-clickhouse model has three layers of configuration that Snowflake developers don't need to think about:

1. **Materialization** — which physical object does dbt create (view, table, incremental, ephemeral)?
2. **Engine config** — for tables and incremental models, which ClickHouse engine and version column?
3. **Incremental strategy** — for incremental models, how does dbt handle new/updated rows?

There is also a correctness concern unique to ReplacingMergeTree:

4. **FINAL placement** — where in the dbt DAG does `FINAL` go to guarantee deduplicated reads?

---

## Exercise 1: Materialization Selection

For each model below, choose the correct dbt materialization type (`view`, `ephemeral`, `table`, or `incremental`) and write a one-sentence reasoning.

Use the update pattern description to guide your choice:
- Insert-only read from a source? → `view`
- Pure join logic, not queried directly? → `ephemeral`
- Full rebuild on every dbt run, no partial updates? → `table`
- Only new/changed rows processed per run? → `incremental`

Fill in the `?` cells:

| Model | Layer | Update Pattern | Materialization | Why |
|-------|-------|----------------|-----------------|-----|
| `stg_trips` | staging | Read + clean from `trips_raw`; no writes to this model | ? | ? |
| `stg_taxi_zones` | staging | Read + clean from raw `taxi_zones` source (seeded by `scripts/00_seed_zones.sql`); no writes | ? | ? |
| `int_trips_enriched` | intermediate | Joins `stg_trips` with zone, payment, vendor, and date dims | ? | ? |
| `fact_trips` | analytics | One row per trip; trips can be corrected (fare adjustments re-insert same `trip_id`) | ? | ? |
| `agg_hourly_zone_trips` | analytics | dbt recalculates the last 2 hours on every run and re-inserts results | ? | ? |
| `dim_taxi_zones` | analytics | 265 static zones; dbt rebuilds the entire table from scratch on every run | ? | ? |
| `dim_payment_type` | analytics | 6 static types; dbt rebuilds from scratch on every run | ? | ? |
| `dim_vendor` | analytics | 3 vendors; dbt rebuilds from scratch on every run | ? | ? |

---

## Exercise 2: Engine Configuration

Only models with `table` or `incremental` materialization need a ClickHouse engine. Views and ephemeral models have no engine.

For each model below, fill in the `ENGINE` and `Version Column` columns. Reference your answers from Worksheet 1 (engine selection) — the dbt engine config should match the design decisions you already made.

| Model | Materialization | ENGINE | Version Column | Why this engine? |
|-------|----------------|--------|----------------|-----------------|
| `fact_trips` | incremental | ? | ? | ? |
| `agg_hourly_zone_trips` | incremental | ? | ? | ? |
| `dim_taxi_zones` | table | ? | — | ? |
| `dim_payment_type` | table | ? | — | ? |
| `dim_vendor` | table | ? | — | ? |

**Hint:** Cross-check your answers against your completed Section 3 in `migration-plan.md`. The dbt engine config is the implementation of the engine decisions you made in Worksheet 1.

---

## Exercise 3: Incremental Strategy Design

For each incremental model, design the full incremental configuration: `unique_key`, `incremental_strategy`, and the SQL filter that goes inside the `is_incremental()` guard block.

```sql
-- Template for the is_incremental() block:
{% if is_incremental() %}
  WHERE <your filter here>
{% endif %}
```

Fill in the `?` cells:

| Model | `unique_key` | `incremental_strategy` | Incremental filter (inside `is_incremental()`) | Why this filter? |
|-------|-------------|----------------------|------------------------------------------------|-----------------|
| `fact_trips` | ? | ? | `WHERE updated_at > ?` | ? |
| `agg_hourly_zone_trips` | ? | ? | `WHERE pickup_at >= ?` | ? |

**Hints:**
- `fact_trips` uses a high-watermark pattern — it processes rows newer than the latest row already in the target. Which column should the watermark be on: `pickup_at` or `updated_at`? Think about what happens when a fare adjustment re-inserts the same `trip_id`.
- `agg_hourly_zone_trips` produces hourly aggregates. If you used a high-watermark like `fact_trips`, what would happen to the boundary hour that was partially calculated in the previous run?
- For `agg_hourly_zone_trips`, why is the composite key `[hour_bucket, zone_id]` correct here, while `fact_trips` uses only `trip_id`?

---

## Exercise 4: FINAL Placement

ReplacingMergeTree deduplication is asynchronous. `FINAL` forces it at read time — but placing FINAL in the wrong model has correctness or performance consequences.

For each model below, decide whether `FINAL` should appear in the model's `FROM` clause (i.e., `FROM source FINAL` or `FROM {{ ref(...) }} FINAL`), and explain why:

| Model | Reads from | FINAL in this model's FROM clause? | Why? |
|-------|-----------|-------------------------------------|------|
| `stg_trips` | `trips_raw` (ReplacingMergeTree) | ? | ? |
| `int_trips_enriched` | `stg_trips` (view) | ? | ? |
| `fact_trips` | `int_trips_enriched` (ephemeral) | ? | ? |

**Hints:**
- `trips_raw` is ReplacingMergeTree — it can have duplicate `trip_id` rows from migration script retries or post-cutover producer retries. Where should this be resolved?
- `stg_trips` is a view, not an RMT table. Does reading from a view require FINAL?
- `fact_trips` uses `delete_insert`. Does that mean the table is already clean without FINAL?

---

## Reflection Questions

Answer these before checking the answer key:

1. `int_trips_enriched` could be a `view` materialization (it reads from staging views). Why is `ephemeral` a better choice here?

2. `agg_hourly_zone_trips` recalculates using `now() - INTERVAL 2 HOUR`. What would happen if you used `max(pickup_at)` instead (the same pattern as `fact_trips`)?

3. You have `delete_insert` on `fact_trips` and FINAL in `stg_trips`. What would happen if you removed FINAL from `stg_trips` but kept delete_insert on `fact_trips`?

---

<details>
<summary>▶ Answer Key — try the exercises first before expanding</summary>

### Exercise 1: Materialization Selection

| Model | Materialization | Why |
|-------|-----------------|-----|
| `stg_trips` | `view` | Reads and cleans source data; no updates to this model; views have zero storage cost and are always fresh |
| `stg_taxi_zones` | `view` | Same as stg_trips — passthrough cleaning of a source table |
| `int_trips_enriched` | `ephemeral` | Pure join logic consumed only by `fact_trips`; no need for a physical table; inlined as a CTE avoids an unnecessary database object |
| `fact_trips` | `incremental` | Trips can be corrected after the fact; only new and updated rows should be processed per run |
| `agg_hourly_zone_trips` | `incremental` | Rolling 2-hour recalculation is an incremental pattern — process recent rows, not all 50M |
| `dim_taxi_zones` | `table` | 265 static zones; full rebuild on every dbt run; no partial updates needed |
| `dim_payment_type` | `table` | 6 static types; same reasoning as dim_taxi_zones |
| `dim_vendor` | `table` | 3 vendors; same reasoning |

### Exercise 2: Engine Configuration

| Model | ENGINE | Version Column | Why |
|-------|--------|----------------|-----|
| `fact_trips` | `ReplacingMergeTree(updated_at)` | `updated_at` | Trips can be corrected; `updated_at` set to `now()` on each insert means the latest correction wins during RMT dedup |
| `agg_hourly_zone_trips` | `ReplacingMergeTree(updated_at)` | `updated_at` | Rolling recalculation re-inserts aggregates for (hour_bucket, zone_id) pairs; RMT ensures stale aggregates are removed on background merge |
| `dim_taxi_zones` | `MergeTree()` | — | Full reload by dbt means atomic table swap (full rebuild) on every run; no duplicate rows can accumulate; no dedup needed |
| `dim_payment_type` | `MergeTree()` | — | Same as dim_taxi_zones |
| `dim_vendor` | `MergeTree()` | — | Same as dim_taxi_zones |

### Exercise 3: Incremental Strategy Design

| Model | `unique_key` | `incremental_strategy` | Incremental filter | Why |
|-------|-------------|----------------------|-------------------|-----|
| `fact_trips` | `trip_id` | `delete_insert` | `WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})` | High-watermark on `updated_at` processes both new trips and corrected trips (fare adjustments re-insert the same `trip_id` with the same `pickup_at` but a newer `updated_at`). A `pickup_at` watermark would silently miss corrections — the pickup time never changes. |
| `agg_hourly_zone_trips` | `[hour_bucket, zone_id]` | `delete_insert` | `WHERE pickup_at >= now() - INTERVAL 2 HOUR` | Rolling window re-aggregates the last 2 hours on every run; this ensures the current boundary hour is fully recalculated even if only partial data was available at the previous run's cutoff |

**Why `[hour_bucket, zone_id]` for agg, not just `trip_id`?**
`agg_hourly_zone_trips` has one row per `(hour_bucket, zone_id)` pair, not one row per trip. The composite key `[hour_bucket, zone_id]` is the natural unique key. `delete_insert` deletes all (hour_bucket, zone_id) pairs that appear in the new batch, then inserts the freshly computed aggregates.

**Why `now() - INTERVAL 2 HOUR` breaks with `max(pickup_at)`?**
If `agg_hourly_zone_trips` used `max(pickup_at)` as its filter, it would process only trips with `pickup_at > (last run's max)`. Trips from the current incomplete hour would be counted in one run, then never re-aggregated — the partial hour count would be permanently wrong. The rolling 2-hour window forces re-computation of recent hours so that boundary hours are always complete.

### Exercise 4: FINAL Placement

| Model | FINAL in FROM clause? | Why |
|-------|----------------------|-----|
| `stg_trips` | **Yes** — `FROM trips_raw FINAL` | `trips_raw` is ReplacingMergeTree; it can have duplicate `trip_id` rows from the bulk-load + CDC overlap window. `stg_trips` is the single enforcement point: deduplicate here so every downstream model sees clean data |
| `int_trips_enriched` | **No** | `int_trips_enriched` reads from `stg_trips`, which is a view (not an RMT table). FINAL is irrelevant for views. |
| `fact_trips` | **No** (in the model itself) | `delete_insert` keeps `fact_trips` clean — there should be no duplicates after a completed run. Dashboards and dbt tests that query `fact_trips` directly use `FINAL` externally as a safety net. Adding FINAL inside the model's SELECT would also apply it to the `is_incremental()` subquery that reads `max(pickup_at)` from `{{ this }}` — unnecessary overhead on every incremental run. |

### Reflection Answers

1. **Why ephemeral over view for `int_trips_enriched`?**
A view would create a database object that materializes the JOIN on every downstream query. Since `int_trips_enriched` is only used by `fact_trips`, an ephemeral model inlines the JOIN as a CTE directly inside `fact_trips`'s query plan — one execution, no extra round-trips, no physical object to maintain.

2. **What breaks with `max(pickup_at)` for `agg_hourly_zone_trips`?**
With a high-watermark filter, only trips with `pickup_at > last_run_max` would be processed. Trips arriving in the current incomplete hour were partially counted in the previous run (up to `last_run_max`), then frozen. When new trips arrive for that same hour, they would not be re-aggregated — the boundary hour's count would be permanently understated. The rolling 2-hour window forces re-computation of recent hours so partial hours are always corrected.

3. **What happens if you remove FINAL from `stg_trips`?**
Without FINAL, `stg_trips` may return two rows for the same `trip_id` if `trips_raw` has an unmerged duplicate from the bulk + CDC overlap. `int_trips_enriched` would join those two rows, producing two enriched rows for the same trip. `delete_insert` on `fact_trips` would then delete and re-insert both — so `fact_trips` would also have two rows for that `trip_id`. They would eventually be deduplicated by RMT's background merge, but until then, SUM(fare_amount) would double-count corrected trips and `dbt test` with `unique` on `trip_id` would fail.

</details>

---

## Transfer to migration-plan.md

Once you have completed this worksheet, fill in Section 10 of `migration-plan.md` with your answers and check off:

```
- [ ] dbt model design: completed
```

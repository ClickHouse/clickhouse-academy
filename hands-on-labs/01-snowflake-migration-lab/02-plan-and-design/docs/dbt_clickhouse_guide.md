# dbt-ClickHouse Patterns — Reference Guide

This guide covers the dbt-clickhouse-specific patterns you will use in Part 3. Read it after completing Worksheets 1–4 and before Worksheet 5 (dbt Model Design).

If you are coming from dbt-snowflake, most of the dbt concepts are identical — sources, refs, tests, macros, the staging/intermediate/analytics layer pattern. What changes is the ClickHouse-specific configuration layer: engine, order_by, incremental strategy, and FINAL semantics.

---

## 1. Materialization Types

dbt-clickhouse supports five materializations. Choose based on update pattern, not preference.

| Materialization | Physical object | When to use |
|----------------|----------------|-------------|
| `view` | ClickHouse view | Staging models: clean and type-cast source data; no storage cost; rebuilt on every query |
| `ephemeral` | No object (inlined as CTE) | Intermediate models that combine multiple staging models via JOIN; avoids creating a redundant physical table |
| `table` | Builds full replacement in a staging relation, then atomically swaps it into place via `EXCHANGE TABLES` (or rename-pair on older versions); old table dropped after swap | Small dimension tables that are fully replaced on every dbt run; no partial updates needed. **Note:** full rebuild is infeasible for large tables — use `incremental` for any table over a few thousand rows. |
| `incremental` | `CREATE TABLE` on first run; selective UPDATE pattern on subsequent runs | Fact tables and pre-aggregation tables where only new/changed rows should be processed each run |
| `materialized_view` | ClickHouse Materialized View | Auto-refreshing aggregates; not the same as dbt's incremental — ClickHouse MV updates on every INSERT to the source table |

**Key difference from Snowflake:** dbt-snowflake handles storage details internally. In dbt-clickhouse, `table` and `incremental` models require explicit `+engine` configuration — dbt uses this to generate the `CREATE TABLE ... ENGINE = ...` DDL.

**Views have no engine.** If you accidentally add `+engine` to a `view` materialization, dbt-clickhouse will ignore it. Only `table` and `incremental` materializations create persistent storage that needs an engine.

---

## 2. Expressing ClickHouse Configs in dbt

ClickHouse-specific settings are expressed as dbt model configs, either in `dbt_project.yml` (for project-wide defaults) or in a model's `config()` block (for model-specific overrides).

### In `dbt_project.yml`

```yaml
models:
  your_project:
    analytics:
      +schema: analytics
      +materialized: table
      +engine: "MergeTree()"          # default for all analytics tables

      fact_trips:
        +materialized: incremental
        +engine: "ReplacingMergeTree(updated_at)"   # overrides the default
        +incremental_strategy: delete_insert
        +unique_key: trip_id
        +order_by: "(toStartOfMonth(pickup_at), pickup_at, trip_id)"
```

### In a model's `config()` block

```sql
{{ config(
    materialized         = 'incremental',
    engine               = 'ReplacingMergeTree(updated_at)',
    incremental_strategy = 'delete_insert',
    unique_key           = 'trip_id',
    order_by             = '(toStartOfMonth(pickup_at), pickup_at, trip_id)'
) }}
```

Both approaches are equivalent. `dbt_project.yml` is preferred for project-wide patterns; `config()` blocks are preferred for model-specific overrides or when you want the configuration co-located with the SQL.

### Key config parameters

| Parameter | What it controls | ClickHouse mapping |
|-----------|-----------------|-------------------|
| `+engine` | Table storage engine | `ENGINE = ...` in CREATE TABLE |
| `+order_by` | Primary key / sort order | `ORDER BY ...` in CREATE TABLE; defaults to `tuple()` if omitted |
| `+unique_key` | Key for delete_insert dedup | Determines which rows to delete before inserting |
| `+incremental_strategy` | How incremental runs update data | Set to `delete_insert` for ClickHouse |

**Scoping rules:** Settings in `dbt_project.yml` cascade from parent to child. A model-level `config()` block always wins over the project config. Set the most common engine as the project default, then override for the models that differ.

---

## 3. `delete_insert` Mechanics

`delete_insert` is the dbt-clickhouse community's standard incremental strategy. It is the closest equivalent to Snowflake's `MERGE INTO` — but the mechanics are different.

> **Version requirement:** `delete_insert` uses ClickHouse lightweight deletes, introduced in 22.8 (experimental) and production-ready in 23.3+. ClickHouse Cloud meets this requirement. To enable it, add `use_lw_deletes: true` to the ClickHouse target in your `~/.dbt/profiles.yml`, or set `allow_experimental_lightweight_delete=1` in `query_settings`.

### What it does

On each incremental run:

1. **DELETE** rows from the target table where `unique_key` matches any row in the incoming batch
2. **INSERT** all rows from the incoming batch

```sql
-- Step 1: dbt generates this DELETE
ALTER TABLE analytics.fact_trips
DELETE WHERE trip_id IN (SELECT trip_id FROM incoming_batch);

-- Step 2: dbt generates this INSERT
INSERT INTO analytics.fact_trips
SELECT * FROM incoming_batch;
```

### How it differs from Snowflake `MERGE INTO`

Snowflake's `merge` strategy generates row-by-row `WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT`. ClickHouse has no `MERGE INTO` statement. `delete_insert` achieves the same end result — one row per unique key — via a batch delete followed by a full insert.

### How it interacts with ReplacingMergeTree

`delete_insert` is the **primary correctness path**. ReplacingMergeTree is the **safety net**.

If a `delete_insert` run completes normally: the table is clean (one row per `trip_id`), no duplicates.

If a `delete_insert` run is interrupted mid-flight (crash after DELETE, before INSERT): the data is likely to be in an invalid state — rows that were deleted may not have been re-inserted. The next successful run will restore the correct state, but do not query the table between a failed DELETE and its re-run.

If a run produces duplicates for any reason: ReplacingMergeTree's background merge will eventually deduplicate them, keeping the row with the highest version column value.

Never rely on RMT alone without `delete_insert` — background merges are asynchronous and can take minutes to hours on large tables.

### When to use `append`

`append` inserts new rows without touching existing ones. It is the correct strategy for purely insert-only tables where rows are never updated — for example, an immutable event log or a raw ingest table with guaranteed-unique IDs and no corrections. `append` has no version requirement and no mutation risk.

For `fact_trips`, `append` is wrong: a trip can be corrected after the fact (fare adjustment, status change), so the same `trip_id` arrives again with new values. With `append`, both versions accumulate permanently, and aggregates (SUM of fares, COUNT of trips) over-count until the next background RMT merge. Use `delete_insert` whenever rows can be updated.

### Why not `merge` strategy?

The `merge` strategy (the legacy default before `delete_insert`) creates a temporary table, populates it with the unchanged existing rows plus the new batch, then atomically replaces the original table. Unlike `delete_insert`, it does not use lightweight deletes — it rewrites the entire table on every incremental run. For a 50M-row `fact_trips` table this would be extremely expensive. `delete_insert` processes only the rows in the current batch; `merge` touches every row in the table. Use `delete_insert`.

---

## 4. FINAL Placement Strategy

ReplacingMergeTree deduplication happens in the background — ClickHouse merges parts asynchronously. Between merges, duplicate rows coexist. `FINAL` forces synchronous deduplication at read time.

### Where FINAL belongs in a dbt pipeline

**At the layer that reads from a ReplacingMergeTree source and produces clean analytical data.**

For the NYC Taxi workload:

```
trips_raw (RMT)
    ↓
stg_trips (view): SELECT ... FROM trips_raw FINAL   ← FINAL goes here
    ↓
int_trips_enriched (ephemeral CTE)
    ↓
fact_trips (incremental, RMT)                        ← NO FINAL in model
    ↓
Dashboard queries: SELECT ... FROM fact_trips FINAL  ← FINAL goes here (externally)
```

`stg_trips` is the single enforcement point for `trips_raw` deduplication. Every downstream model that reads `stg_trips` automatically gets clean, deduplicated source data. You don't need FINAL in `int_trips_enriched` or `fact_trips` because they read from `stg_trips` (a view, not an RMT table).

Dashboard queries and dbt tests that read directly from `fact_trips` use FINAL externally. The model itself doesn't embed FINAL because it would apply to every scan inside the model's query — including the `is_incremental()` subquery that reads `max(updated_at)` from `{{ this }}`.

### Performance impact of FINAL

`FINAL` adds latency proportional to the number of duplicate rows. On a well-maintained RMT table (frequent background merges), `FINAL` adds minimal overhead because there are few duplicates to resolve. On a freshly loaded table with many unmerged parts, `FINAL` can be significantly slower.

For dbt tests and verification queries, always use FINAL on RMT tables. For benchmark queries where latency comparison against Snowflake is the point, the ClickHouse queries already use FINAL — so the comparison is fair.

---

## 5. `generate_schema_name` Macro

By default, dbt prefixes model schemas with the target schema name from the profile. If your dbt profile targets schema `nyc_taxi_ch`, a model with `+schema: analytics` lands in `nyc_taxi_ch_analytics` — not `analytics`.

This is harmless in Snowflake (schemas are namespaces within a database) but creates awkward names in ClickHouse where schemas *are* databases. `nyc_taxi_ch_analytics` is a valid ClickHouse database name, but it's uglier than `analytics` and doesn't match the target database names used in Part 3's ClickHouse architecture.

The fix is a `generate_schema_name` macro override:

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema | lower }}
  {%- else -%}
    {{ custom_schema_name | lower }}
  {%- endif -%}
{%- endmacro %}
```

This macro:
- Returns `custom_schema_name` as-is (lowercased) when a model specifies `+schema: analytics`
- Returns the profile's target schema (lowercased) for models with no custom schema

The `| lower` filter also ensures schema names are consistently lowercase, matching ClickHouse's case-sensitive identifier rules (Snowflake Part 1 used `| upper`).

**Where it lives:** `macros/generate_schema_name.sql` — in the top-level `macros/` directory; `dbt_project.yml` sets `macro-paths: ["macros"]`.

---

## Putting It Together: NYC Taxi dbt Config Summary

```yaml
# dbt_project.yml (abbreviated)
models:
  nyc_taxi_dbt_ch:
    staging:
      +schema: staging
      +materialized: view           # no engine — views need none

    intermediate:
      +schema: staging
      +materialized: ephemeral      # inlined as CTE

    analytics:
      +schema: analytics
      +materialized: table
      +engine: "MergeTree()"        # default for dim_* tables

      fact_trips:
        +materialized: incremental
        +engine: "ReplacingMergeTree(updated_at)"
        +incremental_strategy: delete_insert
        +unique_key: trip_id

      agg_hourly_zone_trips:
        +materialized: incremental
        +engine: "ReplacingMergeTree(updated_at)"
        +incremental_strategy: delete_insert
        +unique_key: [hour_bucket, zone_id]
```

```sql
-- stg_trips.sql (staging view — the FINAL enforcement point)
SELECT ... FROM {{ source('raw', 'trips_raw') }} FINAL

-- fact_trips.sql (incremental — no FINAL in model body)
SELECT ... FROM {{ ref('int_trips_enriched') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
```

# ClickHouse Concepts Guide

This document explains the ClickHouse concepts you will encounter in this migration lab — what they are, why they exist, and how they differ from the Snowflake constructs you used in Part 1.

---

## 1. Table Engines

ClickHouse is not a single-engine database. Every table you create must declare its **engine**, which determines how data is stored on disk, how duplicates are handled, and what capabilities are available. Choosing the wrong engine is the most common mistake in ClickHouse schema design.

### MergeTree

The base engine for almost all production tables.

```sql
CREATE TABLE analytics.dim_taxi_zones (
    zone_id      UInt16,
    borough      String,
    service_zone String
) ENGINE = MergeTree()
ORDER BY zone_id;
```

**What it does:** ClickHouse stores data in **parts** — sorted, compressed chunks on disk. When you insert data, new parts are written. In the background, ClickHouse continuously **merges** smaller parts into larger ones, keeping the data sorted by the `ORDER BY` key. This is where the name comes from.

**When to use:** Any table where you don't need deduplication and inserts are append-only or bulk loads (dimension tables, raw event tables, log tables).

**Key property:** There is no primary key enforcement. Two rows with identical `ORDER BY` values are both stored. If you need deduplication, use `ReplacingMergeTree`.

### ReplacingMergeTree(version_col)

The deduplication engine. Extends MergeTree with a rule: during a background merge, if two rows share the same `ORDER BY` key, keep only the one with the highest `version_col` value.

```sql
CREATE TABLE analytics.fact_trips (
    trip_id     String,
    pickup_at   DateTime,
    total_amount Float64,
    updated_at  DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (pickup_at, trip_id);
```

**Important — eventual consistency:** Deduplication only happens during background merges. At any given moment, your table may contain duplicate rows. This is called **eventual consistency**. To get fully deduplicated results at query time, add `FINAL` to your SELECT:

```sql
-- Without FINAL: may return duplicates if merges haven't run
SELECT * FROM analytics.fact_trips WHERE trip_id = 'abc';

-- With FINAL: forces deduplication at query time (slower, always correct)
SELECT * FROM analytics.fact_trips FINAL WHERE trip_id = 'abc';
```

**How dbt uses it:** The dbt-clickhouse adapter uses the `delete_insert` incremental strategy as its primary mechanism — it explicitly deletes rows with matching keys and inserts fresh ones, which is always correct. `ReplacingMergeTree` acts as a **safety net** that cleans up any duplicates that slipped through (e.g., from a failed partial insert).

**Equivalent to Snowflake:** There is no direct equivalent. In Snowflake you used `MERGE INTO ... WHEN MATCHED THEN UPDATE`. ClickHouse has no `MERGE` statement — `ReplacingMergeTree` plus `FINAL` achieves the same logical result.

### Refreshable Materialized View

ClickHouse supports two kinds of materialized views.

**Trigger-based MV** (traditional): Executes on every INSERT, processes only the newly inserted batch.

```sql
-- Trigger-based: only sees the rows inserted in the current batch
CREATE MATERIALIZED VIEW analytics.mv_realtime_counts
TO analytics.counts_table AS
SELECT pickup_date, count() AS trips
FROM default.trips_raw
GROUP BY pickup_date;
```

**Refreshable MV** (scheduled): Re-runs the full query on a schedule, like a cron job.

```sql
-- Refreshable: runs the full SELECT every 3 minutes
CREATE MATERIALIZED VIEW analytics.mv_hourly_revenue
REFRESH EVERY 180 SECOND AS
SELECT
    toStartOfHour(pickup_at) AS hour_bucket,
    pickup_borough,
    sum(total_amount)        AS revenue
FROM analytics.fact_trips FINAL
GROUP BY hour_bucket, pickup_borough;
```

**When to use which:**
- **Trigger-based**: real-time aggregations on insert streams where you only need to process new data
- **Refreshable**: aggregations that query `fact_trips FINAL` (must see the full table to deduplicate), or dashboards that can tolerate a few minutes of staleness in exchange for simpler logic

**Modifying the refresh interval:**

```sql
ALTER TABLE analytics.mv_hourly_revenue MODIFY REFRESH EVERY 60 SECOND;
```

---

## 2. Sorting Keys (ORDER BY)

In Snowflake, you used `CLUSTER BY` as a hint to the optimizer. In ClickHouse, `ORDER BY` is the **primary index** — it determines the physical sort order of data on disk and drives all range scans.

### How it works

ClickHouse stores a **sparse primary index**: one index entry per ~8,192 rows (one data granule). When you filter by the `ORDER BY` columns, ClickHouse skips entire granules without reading them. This is why ClickHouse can scan billions of rows per second — most of the data never leaves disk.

### Cardinality order matters

Always place **low-cardinality columns first**, **high-cardinality columns last**. This gives the index maximum skipping power for the common case.

```sql
-- Good: low cardinality (borough, ~6 values) first, then high cardinality (trip_id)
ORDER BY (pickup_borough, toStartOfMonth(pickup_at), trip_id)

-- Bad: high cardinality first — the index can't skip anything useful
ORDER BY (trip_id, pickup_borough, pickup_at)
```

### Snowflake CLUSTER BY vs ClickHouse ORDER BY

| Feature | Snowflake `CLUSTER BY` | ClickHouse `ORDER BY` |
|---------|----------------------|----------------------|
| Purpose | Query performance hint | Physical sort order (required) |
| Enforcement | Background reclustering (async) | Always enforced on insert |
| Scope | Micro-partitions | Data granules (~8K rows) |
| Mandatory | No | Yes — every MergeTree table must have one |

### Example: matching an existing Snowflake cluster key

```sql
-- Snowflake
CLUSTER BY (DATE_TRUNC('month', PICKUP_AT), PICKUP_LOCATION_ID)

-- ClickHouse equivalent
ORDER BY (toStartOfMonth(pickup_at), pickup_location_id, trip_id)
-- Note: trip_id added as tiebreaker to ensure unique sort order
```

### Skip indexes (brief mention)

For columns not in the `ORDER BY` key, ClickHouse supports **skip indexes** (bloom filter, minmax, set) that store column-level metadata per granule. Useful for filtering on low-cardinality columns that appear after a high-cardinality column in the sort key.

```sql
-- Add a bloom filter skip index on payment_type
ALTER TABLE analytics.fact_trips
ADD INDEX idx_payment_type payment_type TYPE bloom_filter GRANULARITY 4;
```

---

## 3. JSON Handling

Snowflake's `VARIANT` column type supports colon-path notation to traverse nested JSON. ClickHouse uses explicit `JSONExtract*` functions instead.

### Side-by-side translation table

| Snowflake | ClickHouse | Notes |
|-----------|------------|-------|
| `col:key::FLOAT` | `JSONExtractFloat(col, 'key')` | Top-level float field |
| `col:driver.rating::FLOAT` | `JSONExtractFloat(col, 'driver', 'rating')` | Nested float field |
| `col:app.surge_multiplier::FLOAT` | `JSONExtractFloat(col, 'app', 'surge_multiplier')` | Nested float |
| `col:route.waypoints[0]::STRING` | `JSONExtractString(col, 'route', 'waypoints', 0)` | Array element by index |
| `col:driver.id::INT` | `JSONExtractInt(col, 'driver', 'id')` | Integer field |

### Function variants

```sql
-- Float (returns 0.0 if key missing or wrong type)
JSONExtractFloat(trip_metadata, 'driver', 'rating')

-- String (returns '' if missing)
JSONExtractString(trip_metadata, 'app', 'version')

-- Integer (returns 0 if missing)
JSONExtractInt(trip_metadata, 'driver', 'id')

-- Bool (returns 0/1)
JSONExtractBool(trip_metadata, 'app', 'is_shared')

-- Raw value as string (preserves JSON sub-object)
JSONExtractRaw(trip_metadata, 'route')
```

### Performance tip

If you query the same JSON column repeatedly, consider extracting fields into typed columns at the staging model level (in `stg_trips.sql`) rather than calling `JSONExtractFloat` in every downstream query. This is what the lab's dbt models do.

---

## 4. Date/Time Functions

Snowflake and ClickHouse have similar date/time capabilities but different syntax. The most common translations:

### Side-by-side translation table

| Snowflake | ClickHouse | Notes |
|-----------|------------|-------|
| `DATE_TRUNC('hour', col)` | `toStartOfHour(col)` | Truncate to hour |
| `DATE_TRUNC('day', col)` | `toStartOfDay(col)` or `toDate(col)` | Truncate to day |
| `DATE_TRUNC('month', col)` | `toStartOfMonth(col)` | Truncate to month |
| `CURRENT_TIMESTAMP()` | `now()` | Current datetime |
| `CURRENT_DATE()` | `today()` | Current date |
| `DATEADD('day', -7, CURRENT_DATE())` | `today() - INTERVAL 7 DAY` | Date arithmetic |
| `DATEDIFF('day', a, b)` | `dateDiff('day', a, b)` | Days between two dates |

### Additional ClickHouse-only conveniences

```sql
yesterday()              -- today() - 1 day
toStartOfWeek(col)       -- Monday of the containing week
toStartOfQuarter(col)    -- first day of the quarter
toYear(col)              -- extract year as integer
toMonth(col)             -- extract month as integer (1-12)
toDayOfWeek(col)         -- 1=Monday, 7=Sunday
```

### Interval syntax

```sql
-- ClickHouse
now() - INTERVAL 7 DAY
now() - INTERVAL 1 HOUR
now() - INTERVAL 30 MINUTE
pickup_at + INTERVAL 90 SECOND

-- Snowflake equivalent
DATEADD('day', -7, CURRENT_TIMESTAMP())
DATEADD('hour', -1, CURRENT_TIMESTAMP())
```

---

## 5. Approximate Functions

ClickHouse is built for analytical workloads where exact answers on billions of rows are slower than approximate answers that are accurate enough for dashboards. ClickHouse ships with several built-in approximate aggregate functions.

### Count distinct

| Function | Accuracy | Speed | When to use |
|----------|----------|-------|-------------|
| `uniqExact(col)` | Exact | Slowest | Compliance reports, invoicing |
| `uniq(col)` | ~2% error | Fast | Dashboards, exploration |
| `uniqHLL12(col)` | ~1.6% error | Fastest, fixed 2.5KB memory | High-cardinality, memory-constrained |

```sql
-- Exact (like Snowflake COUNT(DISTINCT ...))
SELECT uniqExact(trip_id) FROM analytics.fact_trips FINAL;

-- Approximate — good for "how many unique passengers today?"
SELECT uniq(passenger_id) FROM analytics.fact_trips FINAL;
```

### Percentiles

| Function | Notes |
|----------|-------|
| `quantile(level)(col)` | Exact quantile, memory-intensive |
| `quantileTDigest(level)(col)` | Approximate using t-digest, fixed memory |
| `quantileTDigestWeighted(level)(col, weight)` | Weighted t-digest |

```sql
-- P95 trip duration — approximate but uses O(1) memory
SELECT quantileTDigest(0.95)(duration_minutes)
FROM analytics.fact_trips FINAL;

-- Multiple percentiles in one pass
SELECT quantileTDigestMerge(0.5)(state), quantileTDigestMerge(0.95)(state)
FROM analytics.fact_trips FINAL;
```

**Rule of thumb:** Use `uniq` and `quantileTDigest` for interactive dashboards. Use `uniqExact` and `quantile` only when you need exact values for billing, SLAs, or compliance.

---

## 6. Dictionaries

Dictionaries are **in-memory lookup tables** that ClickHouse keeps hot and pre-joined at query time. They are the ClickHouse equivalent of a small dimension table that you want to join without the cost of a full JOIN.

### What they are

A dictionary is backed by a source (a ClickHouse table, a file, or an external database) and loaded into memory when the service starts or when you call `SYSTEM RELOAD DICTIONARIES`. Lookups happen via a key, returning one or more attributes.

### CREATE DICTIONARY syntax

```sql
-- From 04_create_dictionary.sql
CREATE DICTIONARY analytics.taxi_zones_dict (
    zone_id      UInt16,
    borough      String,
    service_zone String
)
PRIMARY KEY zone_id
SOURCE(CLICKHOUSE(
    TABLE 'dim_taxi_zones'
    DB    'analytics'
))
LIFETIME(MIN 300 MAX 600)   -- refresh every 5-10 minutes
LAYOUT(FLAT());             -- hash map, best for < 1M rows
```

**LAYOUT options:**
- `FLAT()` — array indexed by integer key, fastest, requires sequential integer keys
- `HASHED()` — hash map, works with any integer key
- `COMPLEX_KEY_HASHED()` — hash map with composite or string keys

### dictGet usage

```sql
-- Instead of: JOIN analytics.dim_taxi_zones USING (zone_id)
SELECT
    trip_id,
    dictGet('analytics.taxi_zones_dict', 'borough', toUInt64(pickup_location_id)) AS pickup_borough,
    dictGet('analytics.taxi_zones_dict', 'borough', toUInt64(dropoff_location_id)) AS dropoff_borough
FROM analytics.fact_trips FINAL;
```

### When to use dictionaries vs JOINs

| Scenario | Use |
|----------|-----|
| Small, stable reference table (< 1M rows, rarely changes) | Dictionary |
| Large dimension table or frequently updated data | JOIN |
| Dashboard query that runs repeatedly with the same lookup | Dictionary (lookup is free after first load) |
| One-off analytical query | JOIN |

---

## 7. SAMPLE Clause

ClickHouse supports row-level sampling directly in the query syntax. Sampling reads a deterministic fraction of the data — useful for exploratory analysis when you don't need exact results.

### Syntax

```sql
-- Read approximately 10% of rows
SELECT count(), avg(total_amount)
FROM analytics.fact_trips SAMPLE 0.1;

-- Read a specific number of rows (approximately)
SELECT trip_id, pickup_at, total_amount
FROM analytics.fact_trips SAMPLE 1000000;
```

### Scaling results

When sampling, multiply aggregates by `1 / sample_rate` to estimate full-table values:

```sql
-- Estimate total revenue from 10% sample
SELECT sum(total_amount) * 10 AS estimated_total_revenue
FROM analytics.fact_trips SAMPLE 0.1;
```

### When to use SAMPLE

- Exploratory analysis ("is my query logic correct?") before running on the full table
- Dashboard tiles where approximate values are acceptable
- Training ML models on a representative subset

**Note:** SAMPLE requires the table `ORDER BY` key to start with the sampling column, or you must add a `SAMPLE BY` clause to the `CREATE TABLE` statement. The lab's `trips_raw` table is created with `SAMPLE BY cityHash64(trip_id)` for this purpose.

---

## 8. dbt-clickhouse Adapter Notes

The dbt-clickhouse adapter (`dbt-clickhouse>=1.8`) supports most standard dbt features but has some ClickHouse-specific behaviour you need to understand.

### `delete_insert` incremental strategy

ClickHouse has no `MERGE INTO`. The dbt-clickhouse adapter's `delete_insert` strategy emulates it:

1. Delete rows from the target table where the key column(s) match the incoming batch
2. Insert the full set of incoming rows

```sql
-- What dbt generates for incremental models
DELETE FROM analytics.fact_trips WHERE trip_id IN (SELECT trip_id FROM __dbt_tmp);
INSERT INTO analytics.fact_trips SELECT * FROM __dbt_tmp;
```

Configure it in your model:

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='delete_insert',
        unique_key='trip_id',
        engine='ReplacingMergeTree(updated_at)',
        order_by='(pickup_at, trip_id)'
    )
}}
```

### Engine and order_by config

Every MergeTree table requires an engine and an ORDER BY. Specify both in the dbt model config:

```sql
{{
    config(
        engine='MergeTree()',
        order_by='(zone_id)'
    )
}}
```

### `order_by` instead of `cluster_by`

In Snowflake dbt models you may have used `cluster_by`. In dbt-clickhouse, use `order_by` instead. There is no equivalent to Snowflake's cluster_by in ClickHouse — `ORDER BY` is always the physical sort.

### `profiles.yml` for ClickHouse Cloud

ClickHouse Cloud requires TLS. Set `secure: true`:

```yaml
# ~/.dbt/profiles.yml
nyc_taxi_ch:
  target: dev
  outputs:
    dev:
      type: clickhouse
      host: "{{ env_var('CLICKHOUSE_HOST') }}"
      port: 8443
      user: default
      password: "{{ env_var('CLICKHOUSE_PASSWORD') }}"
      schema: analytics       # default database for models without a custom schema
      secure: true
      threads: 4
```

### Schema naming and separate databases

ClickHouse uses the term **database** where Snowflake uses **schema**. The dbt-clickhouse adapter maps dbt schemas to ClickHouse databases. The `generate_schema_name` macro in this project overrides dbt's default behaviour so that models with `+schema: analytics` land in the `analytics` database, not `staging_analytics`.

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name }}
  {%- endif -%}
{%- endmacro %}
```

This is the same pattern used in the Snowflake dbt project (Part 1) — the macro is intentionally identical so the schema naming behaviour is consistent across both adapters.

# Snowflake vs ClickHouse — Architecture & SQL Dialect Reference

This document is a reference for partners migrating from Snowflake to ClickHouse. It covers the architectural differences that drive design decisions and the six SQL dialect gaps you will encounter in the NYC Taxi workload.

---

## 1. Architecture Comparison

### Storage

**Snowflake** makes all physical storage decisions for you. Data is stored as compressed columnar micro-partitions in cloud object storage. You choose a warehouse size and table structure; Snowflake handles everything else — clustering, compaction, and file management are automatic.

**ClickHouse** requires you to make physical storage decisions explicitly. When you create a table, you specify:
- The **engine** (which determines how data is stored, merged, and deduplicated)
- The **ORDER BY** (which becomes the physical sort order and primary index)
- Optionally: **PARTITION BY**, **TTL**, **SETTINGS** (compression codec, merge behavior)

These are correctness decisions, not performance tuning knobs. The wrong engine can produce silently incorrect query results. The wrong ORDER BY can make queries that should be fast scan the entire table instead.

### Query Execution

**Snowflake** uses shared-nothing MPP with virtual warehouses. A warehouse is a cluster of compute nodes that processes queries. You pay for the warehouse while it's running — idle time costs credits. Auto-suspend helps, but cold-start adds latency.

**ClickHouse** uses vectorized execution. ClickHouse Cloud auto-scales each compute service independently and scales to zero when idle. Multiple compute services can share the same storage (via SharedMergeTree) — this is ClickHouse Cloud's compute-compute separation model, where each service is an independent compute tier over a common data layer.

### Concurrency Model

**Snowflake** isolates workloads by creating separate warehouses. ETL uses `TRANSFORM_WH`, analytics uses `ANALYTICS_WH`. Each warehouse has dedicated compute; a slow ETL job cannot starve an analytics query.

**ClickHouse Cloud** supports the same pattern via **compute-compute separation**: you can provision multiple compute services that share the same storage. Each service is an independent autoscaling compute tier — ETL runs on one service, interactive analytics on another, with no resource contention between them. Within a single service, workload isolation is achieved through soft quotas (per-user or per-query `max_threads`, `priority`, `max_memory_usage`) and user profiles with resource limits. For most analytical workloads where queries complete in milliseconds, a single service is sufficient and per-query quotas are the lighter-weight option.

### Cost Model

| | Snowflake | ClickHouse Cloud |
|--|-----------|-----------------|
| Compute | Credits (warehouse-seconds) | Compute units (separate from storage) |
| Storage | $23/TB/month | ~$0.023/GB/month (cheaper) |
| Scale-to-zero | Auto-suspend only | Full scale-to-zero supported |
| Data transfer | Ingress free; egress charged | Standard cloud egress rates |

The most significant difference: in Snowflake, you pay for *warehouse time* regardless of whether queries are running. In ClickHouse Cloud, compute scales down to zero between queries. For bursty analytical workloads, ClickHouse Cloud is typically 3-8x cheaper than an equivalent Snowflake configuration.

---

## 2. SQL Dialect Gaps

The NYC Taxi workload contains six constructs that require translation. Every one of them appears in Q1–Q7 in `01-setup-snowflake/queries/`.

### Gap 1: QUALIFY

`QUALIFY` is a Snowflake extension that filters rows by window function result, similar to how `HAVING` filters by aggregate result. For this migration, we treat `QUALIFY` as a dialect gap and rewrite it using a subquery — this is the universally portable pattern that works across all SQL engines.

```sql
-- Snowflake
SELECT
    trip_id,
    pickup_at,
    fare_amount,
    ROW_NUMBER() OVER (PARTITION BY pickup_location_id ORDER BY fare_amount DESC) AS fare_rank
FROM fact_trips
WHERE pickup_at >= CURRENT_DATE - 7
QUALIFY fare_rank <= 10;

-- ClickHouse: wrap in a subquery
SELECT trip_id, pickup_at, fare_amount, fare_rank
FROM (
    SELECT
        trip_id,
        pickup_at,
        fare_amount,
        ROW_NUMBER() OVER (PARTITION BY pickup_location_id ORDER BY fare_amount DESC) AS fare_rank
    FROM analytics.fact_trips
    WHERE pickup_at >= today() - 7
)
WHERE fare_rank <= 10;
```

> **Why this matters:** QUALIFY appears in Q3. The subquery rewrite is the safe, portable pattern — it works regardless of the target SQL engine and makes the window function result explicit. The danger with any Snowflake-specific syntax is assuming it transfers silently; always test every query before claiming migration is complete.

### Gap 2: VARIANT Colon-Path Syntax

Snowflake's `VARIANT` type uses colon-path notation for nested field access: `column:field.subfield::TYPE`. ClickHouse stores semi-structured data as `String` and extracts at query time using `JSONExtract*` functions.

```sql
-- Snowflake
SELECT
    trip_metadata:driver.rating::FLOAT  AS driver_rating,
    trip_metadata:app.version::STRING   AS app_version,
    trip_metadata:surge_multiplier::FLOAT AS surge
FROM trips_raw;

-- ClickHouse
SELECT
    JSONExtractFloat(trip_metadata, 'driver', 'rating')   AS driver_rating,
    JSONExtractString(trip_metadata, 'app', 'version')    AS app_version,
    JSONExtractFloat(trip_metadata, 'surge_multiplier')   AS surge
FROM default.trips_raw;
```

The full `JSONExtract*` family: `JSONExtractFloat`, `JSONExtractInt`, `JSONExtractString`, `JSONExtractBool`, `JSONExtractKeys`, `JSONExtractArrayRaw`, `JSONExtractRaw`. Use `JSONExtractRaw` when you need a nested object or array as a string for further processing.

> **Why not ClickHouse `JSON` type?** The `JSON` type (previously experimental) is available in recent ClickHouse versions but has different semantics and is not yet production-hardened for all use cases. For a migration lab, `String` + `JSONExtract*` is the safe, understood choice.

### Gap 3: LATERAL FLATTEN

Snowflake's `LATERAL FLATTEN` unnests an array inside a VARIANT column into rows. ClickHouse has no direct equivalent.

```sql
-- Snowflake: explode a VARIANT array into rows
SELECT t.trip_id, f.value:stop_name::STRING AS stop_name
FROM trips_raw t,
LATERAL FLATTEN(input => t.trip_metadata:route_stops) f;

-- ClickHouse Option 1: JSONExtract into Array, then arrayJoin
SELECT
    trip_id,
    arrayJoin(JSONExtract(trip_metadata, 'route_stops', 'Array(String)')) AS stop_name
FROM default.trips_raw;

-- ClickHouse Option 2: Pre-flatten the column during dbt staging
-- In stg_trips.sql, extract all array elements to separate columns
-- or use the dbt model to reshape the data at load time
```

The pre-flatten approach (Option 2) is preferred when the array has a bounded, known schema. `arrayJoin` (Option 1) is preferred for ad-hoc queries or when array length is variable.

### Gap 4: MERGE INTO

Snowflake's `MERGE INTO` is the primary upsert mechanism. ClickHouse has no `MERGE` statement. The correct ClickHouse equivalent depends on the table engine.

```sql
-- Snowflake
MERGE INTO fact_trips t
USING staging_trips s ON t.trip_id = s.trip_id
WHEN MATCHED THEN UPDATE SET t.fare_amount = s.fare_amount, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT VALUES (s.trip_id, s.pickup_at, ...);

-- ClickHouse with ReplacingMergeTree: just INSERT
-- RMT deduplicates by the ORDER BY key during background merges.
-- Use FINAL at query time to get the latest version:
INSERT INTO analytics.fact_trips SELECT * FROM staging_trips;

SELECT * FROM analytics.fact_trips FINAL WHERE trip_id = '...';

-- ClickHouse with dbt delete_insert incremental:
-- dbt handles the upsert by: DELETE WHERE key IN (new batch), then INSERT
-- This is the recommended approach for the analytics layer
```

The `delete_insert` incremental strategy in dbt-clickhouse is the closest semantic equivalent to `MERGE INTO` for analytical models. It deletes existing rows that match any key in the incoming batch, then inserts all incoming rows — atomically per partition.

> **Key gotcha with ReplacingMergeTree:** Background deduplication is asynchronous. Between merges, both the old and new version of a row exist in the table. Always use `FINAL` in queries that must return exactly one row per key. See `docs/mergetree_guide.md` for full deduplication semantics.

### Gap 5: Snowflake Streams (CDC)

Snowflake Streams track row-level changes (INSERT, UPDATE, DELETE) on a table. They expose `METADATA$ACTION`, `METADATA$ISUPDATE`, and `METADATA$ROW_ID` system columns. ClickHouse has no equivalent internal mechanism.

**ClickHouse equivalent: direct producer cutover**

ClickHouse has no internal CDC mechanism equivalent to Snowflake Streams. For this migration, the pattern is simpler than a CDC connector:

- Bulk load first — `scripts/02_migrate_trips.py` reads all historical rows from Snowflake in batches and inserts into ClickHouse
- Then cut over the producer — `scripts/03_cutover.sh` stops the Snowflake producer and starts a ClickHouse producer that writes directly to ClickHouse Cloud
- No CDC window needed — the migration script handles the historical load, and the producer takes over for live writes; `ReplacingMergeTree(_synced_at)` on `trips_raw` makes any migration retries or producer retries idempotent

Post-cutover, the dbt `delete_insert` strategy handles upserts for the analytics layer. Snowflake Streams and Tasks are retired entirely.

### Gap 6: Date/Time Functions

Snowflake and ClickHouse have different date function names. Most are mechanical substitutions.

| Snowflake | ClickHouse | Notes |
|-----------|-----------|-------|
| `DATE_TRUNC('hour', ts)` | `toStartOfHour(ts)` | Also: `toStartOfDay`, `toStartOfMonth`, `toStartOfWeek` |
| `DATE_TRUNC('day', ts)` | `toDate(ts)` | |
| `DATEADD('day', n, ts)` | `ts + INTERVAL n DAY` | Or `addDays(ts, n)` |
| `DATEDIFF('minute', t1, t2)` | `dateDiff('minute', t1, t2)` | Lowercase function name |
| `CURRENT_DATE` | `today()` | |
| `CURRENT_TIMESTAMP()` | `now()` | |
| `TO_TIMESTAMP(epoch, 9)` | `fromUnixTimestamp64Nano(epoch)` | Units explicit in CH |
| `YEAR(ts)` | `toYear(ts)` | |
| `MONTH(ts)` | `toMonth(ts)` | |
| `EXTRACT(epoch FROM ts)` | `toUnixTimestamp(ts)` | |

> **DateTime vs DateTime64:** ClickHouse's `DateTime` has second precision. Use `DateTime64(3, 'UTC')` for millisecond precision (matching Snowflake's `TIMESTAMP_NTZ`). The `3` is the sub-second scale; `'UTC'` is the timezone.

---

## 3. Data Movement Options

| Method | When to use | Notes |
|--------|-------------|-------|
| **Python migration script** (`scripts/02_migrate_trips.py`) | Bulk load for Snowflake → ClickHouse | Direct connection via `snowflake-connector-python` + `clickhouse-connect`; resumable; no additional services needed — **used in this lab** |
| **ClickPipes** | Kafka, S3, Kinesis, PostgreSQL CDC, MySQL CDC | Managed connector; does not support Snowflake as a source |
| **`remoteSecure()`** | Ad-hoc pull from another ClickHouse service | Not applicable for Snowflake source |
| **Object storage relay** | Large one-time loads | Export Snowflake → S3 → ClickHouse S3 table function; requires AWS account and IAM setup |
| **JDBC/ODBC** | Custom ETL pipelines | Flexible but requires custom orchestration |

For this lab, the Python migration script is the correct choice: it requires no additional cloud services (no S3, no Kafka), is fully debuggable, and uses packages (`snowflake-connector-python`, `clickhouse-connect`) that partners already have installed for other lab steps.

---

## 4. CDC Architecture Comparison

| | Snowflake Streams + Tasks | ClickHouse (this lab) |
|--|--------------------------|----------------------|
| Change tracking | Internal stream object on table (`TRIPS_CDC_STREAM`) | No equivalent — producer writes directly to ClickHouse post-cutover |
| Change events | `METADATA$ACTION`: INSERT/UPDATE/DELETE | Direct INSERT from ClickHouse producer |
| Latency | Configurable task schedule (min 1 min) | Configurable batch interval (default 10 s) |
| Consumption | SQL task reads stream, emits to target | Python producer (`producer/producer.py`) |
| Schema changes | Manual coordination | Producer code controls schema |

Post-migration, the producer writes directly to ClickHouse — no Streams or Tasks are needed. The dbt `delete_insert` strategy handles upserts for the analytics layer. Periodic aggregation (Snowflake Tasks) is replaced by Refreshable Materialized Views (`mv_hourly_revenue` refreshes every 3 minutes).

---

## 5. Cost Model Deep Dive

### Snowflake: Credit-Based

A Snowflake credit costs ~$3 (Enterprise). Cost = warehouse_size × time_running. A SMALL warehouse consumes 1 credit/hour. A MEDIUM consumes 2. Auto-suspend at 60 seconds minimum means even a single query costs at least 1/60th of an hour.

For the NYC Taxi lab (X-Small warehouse, 1 credit/hr):
- Part 1 setup: ~2–4 credits (~$6–12)
- Ongoing per 8-hr session: ~4–8 credits/day (~$12–24)
- The ANALYTICS_WH resource monitor caps at 50 credits/month (~$150)

### ClickHouse Cloud: Compute + Storage Separate

ClickHouse Cloud charges separately for compute and storage:
- Compute: Development tier is ~$0.10/hr when active, scales to zero when idle
- Storage: ~$0.023/GB/month (significantly cheaper than Snowflake's $23/TB)
- ClickPipes: included in Cloud subscription for supported sources (Kafka, S3, Kinesis, PostgreSQL CDC, MySQL CDC — not Snowflake)

For the NYC Taxi lab:
- 50M rows × ~300 bytes/row uncompressed = ~15GB → ~8GB compressed in ClickHouse
- Storage cost: ~$0.18/month
- Compute during active Part 3 lab (~2 hrs): ~$0.20–0.40

**Total Part 3 cost: ~$2–4** vs Snowflake's ~$6–12 for the same session.

The cost difference explains why many organizations start with Snowflake (simpler operations) and migrate to ClickHouse (lower cost + higher performance) as their analytical workload scales.

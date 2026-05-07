# MergeTree Engine Family — Deep Dive

ClickHouse stores all data in tables backed by one of the MergeTree engine variants. If you are coming from Snowflake, there is no equivalent concept — Snowflake handles all storage decisions internally. In ClickHouse, choosing the right engine is your responsibility, and choosing wrong produces silently incorrect results.

This guide covers the engines you will use in the NYC Taxi lab and the gotchas that trip up every Snowflake migrator.

---

## What is MergeTree?

MergeTree is ClickHouse's primary storage engine. Data is written to immutable columnar files called *parts*. ClickHouse periodically merges parts in the background — sorting, compressing, and optionally transforming them according to the engine's rules.

The key consequence: **a read may see multiple versions of a row until a merge happens.** Most engines handle this transparently, but some (notably ReplacingMergeTree) require you to understand the merge lifecycle to write correct queries.

When you create a MergeTree table, you must specify `ORDER BY`. This determines:
1. The physical sort order of data within each part
2. The primary index (sparse, block-level, stored in memory)
3. For engines that deduplicate, which columns define the "key" for deduplication

There is no separate concept of a primary key, clustered index, or distribution key. `ORDER BY` is all of these at once.

---

## MergeTree

**Use when:** The table is insert-only or updates are handled externally. No deduplication needed.

```sql
CREATE TABLE default.some_events (
    event_id      String,
    occurred_at   DateTime64(3, 'UTC'),
    payload       String
)
ENGINE = MergeTree()
ORDER BY (occurred_at, event_id);
```

**Characteristics:**
- Inserts append data as new parts
- No deduplication — duplicate rows are preserved
- Merges optimize storage and compression but do not change logical content
- Queries read all parts matching the `ORDER BY` prefix range

**When it goes wrong:** If you insert the same row twice (e.g., retry after a network failure), both rows appear in query results. For truly insert-only pipelines where duplicates cannot occur, this is correct. For any table receiving CDC updates or retryable loads, use ReplacingMergeTree.

---

## ReplacingMergeTree

**Use when:** Rows can be updated (e.g., fare corrections, status changes). You want one row per key in query results.

```sql
CREATE TABLE analytics.fact_trips (
    trip_id       String,
    pickup_at     DateTime64(3, 'UTC'),
    fare_amount   Float64,
    updated_at    DateTime64(3, 'UTC'),
    -- ...
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (toStartOfMonth(pickup_at), pickup_at, trip_id);
```

**Characteristics:**
- During background merges, rows with the same `ORDER BY` key are deduplicated: only the row with the **highest version column value** is kept
- The version column (here `updated_at`) determines which row wins — higher value = newer = kept
- Deduplication is **asynchronous** — until a merge runs, both old and new versions coexist

**The critical gotcha: deduplication lag**

Between merges, a query without `FINAL` sees *all* versions of a row:

```sql
-- This may return multiple rows for the same trip_id
-- if the row has been updated since the last merge
SELECT * FROM analytics.fact_trips WHERE trip_id = 'abc123';

-- This returns exactly one row per trip_id, applying deduplication at query time
SELECT * FROM analytics.fact_trips FINAL WHERE trip_id = 'abc123';
```

`FINAL` forces deduplication at read time. It is slower than reading without `FINAL` because ClickHouse must check all parts for duplicate keys. For the NYC Taxi lab, all queries against `fact_trips` use `FINAL`.

**When it goes wrong:**
- Omitting `FINAL` on a point lookup → silently returns duplicate rows; aggregates over-count
- Using the wrong version column (one that doesn't increase on update) → older values win
- Using MergeTree instead of RMT for a mutable table → all versions accumulate; row count grows unboundedly
- Expecting synchronous deduplication → ETL job reads immediately after insert, sees duplicates

**RMT with dbt:** The `delete_insert` incremental strategy deletes rows in the incoming batch's key range before inserting, so the table never has duplicates in the first place. `FINAL` is still recommended for safety but is less critical when the dbt strategy is correct.

---

## AggregatingMergeTree

**Use when:** The table stores partial aggregation states that should be merged during background merges and combined at query time.

```sql
CREATE TABLE analytics.agg_hourly_revenue (
    hour_bucket   DateTime,
    borough       String,
    fare_sum      AggregateFunction(sum, Float64),
    trip_count    AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (hour_bucket, borough);
```

**Characteristics:**
- Rows with the same `ORDER BY` key are merged using the aggregate function's combiner logic
- Query time uses `-Merge` suffix combiners: `sumMerge(fare_sum)`, `countMerge(trip_count)`
- Typically fed by a Materialized View that converts raw inserts into partial states

**When to use it:** AggregatingMergeTree is for pre-aggregated data where partial states must be combinable. For the NYC Taxi lab, `agg_hourly_zone_trips` is rebuilt by dbt on each run — it is a full-replacement table, not a partial-state accumulator. Use ReplacingMergeTree there.

**When it goes wrong:** Using `sum(fare_sum)` instead of `sumMerge(fare_sum)` at query time treats the binary aggregate state as a Float64 and returns garbage numbers. This is a silent correctness error.

---

## CollapsingMergeTree

**Use when:** You need to delete or update rows by inserting a "sign row" (sign=1 for insert, sign=-1 for cancel). Less common but useful for event-based CDC patterns.

```sql
ENGINE = CollapsingMergeTree(sign)
```

During merges, pairs of rows with sign=1 and sign=-1 for the same key cancel each other out. Not used in the NYC Taxi lab — `ReplacingMergeTree` with a version column is simpler for this workload's insert-retry pattern.

---

## MergeTree with TTL

Add time-based data expiry to any MergeTree variant:

```sql
CREATE TABLE default.trips_raw (
    trip_id    String,
    pickup_at  DateTime64(3, 'UTC'),
    _synced_at DateTime DEFAULT now(),
    -- ...
)
ENGINE = ReplacingMergeTree(_synced_at)
ORDER BY (pickup_at, trip_id)
TTL toDate(pickup_at) + INTERVAL 2 YEAR;
```

TTL triggers during background merges. Expired rows are removed from parts when they merge. For the lab, TTL is not configured — all 4 years of data are retained. In production, TTL is essential for managing storage costs.

---

## Choosing an Engine: Decision Tree

```
Does the table receive UPDATE or DELETE operations?
├── No (insert-only, e.g., event log, append-only stream)
│   └── MergeTree()
└── Yes
    ├── Do rows have a version/timestamp column that increases on update?
    │   ├── Yes → ReplacingMergeTree(version_col)
    │   └── No (full reload, e.g., dim tables rebuilt by dbt)
    │       └── MergeTree() — dbt atomic table swap (full rebuild) handles "upsert"
    └── Is the table a pre-aggregated accumulator with combinable states?
        └── AggregatingMergeTree()
```

**For the NYC Taxi lab:**

| Table | Engine | Reason |
|-------|--------|--------|
| `trips_raw` | ReplacingMergeTree(_synced_at) | Migration script retries and post-cutover producer retries can write the same `trip_id` twice; `_synced_at DEFAULT now()` ensures the later write wins |
| `fact_trips` | ReplacingMergeTree(updated_at) | Trips can be corrected; `updated_at` version |
| `agg_hourly_zone_trips` | ReplacingMergeTree(updated_at) | Rolling recalc = upsert; `updated_at` version |
| `dim_*` tables | MergeTree | Full reload by dbt; no partial updates |
| `mv_hourly_revenue` | Refreshable MV | Runs on a schedule; replaces entire result each time |

---

## ORDER BY Design

The `ORDER BY` is the most important performance decision in a ClickHouse table. It determines:

1. **Primary index efficiency** — queries that filter on `ORDER BY` prefix columns skip irrelevant blocks
2. **Compression ratio** — sorted data compresses better (similar values are adjacent)
3. **Deduplication key** (for RMT/AMT) — two rows are duplicates only if their `ORDER BY` columns match

**Rules for designing ORDER BY:**

1. Put **low-cardinality columns first** (e.g., `borough`, `payment_type`): more rows share a value, so the index skips more blocks
2. Put **high-cardinality columns last** (e.g., `trip_id`, UUID): these narrow the range but cannot compress as well at the front
3. Derive columns from **actual query filters**, not from the source schema
4. For RMT tables, the last column should be the **unique row identifier** (ensures one row per business key)

**Anti-pattern:** Copying the source primary key as ORDER BY. If Snowflake's `TRIPS_RAW` has no explicit sort, copying the Snowflake schema order (`trip_id` first) gives ClickHouse random ORDER BY — no block skipping for any analytical query.

**Example derivation for `fact_trips`:**

Queries Q1–Q7 all filter on `pickup_at` in some form:
- Q1: `WHERE pickup_at >= ...`
- Q2: `ORDER BY week, pickup_location_id`
- Q3: `WHERE pickup_at >= CURRENT_DATE - 7`
- Q4: `GROUP BY DATE_TRUNC('day', pickup_at)`

So `pickup_at` must be in the ORDER BY and should be near the front. Using `toStartOfMonth(pickup_at)` as the first column creates a coarser-grained prefix that enables partition-level pruning even without a PARTITION BY clause. `trip_id` goes last for RMT uniqueness.

Result: `ORDER BY (toStartOfMonth(pickup_at), pickup_at, trip_id)`

---

## PARTITION BY

`PARTITION BY` is optional and separate from `ORDER BY`. It creates physical directory partitions — each partition is an independent set of parts.

```sql
PARTITION BY toYYYYMM(pickup_at)
```

**Use PARTITION BY when:**
- You need to DROP an entire time range efficiently (`ALTER TABLE DROP PARTITION '202401'`)
- You want TTL to operate per-month rather than per-row
- The table is very large (>1TB) and per-partition metadata would benefit query planning

**Do NOT use PARTITION BY to replace ORDER BY.** A common mistake is putting `toYYYYMM(date)` in PARTITION BY and omitting it from ORDER BY — this prevents per-block skipping within a partition.

For the NYC Taxi lab, PARTITION BY is not needed — the dataset is 50M rows (~8GB compressed), well within single-partition performance range.

---

## Key Gotchas Summary

| Gotcha | Consequence | Fix |
|--------|-------------|-----|
| Wrong engine for mutable data | Duplicate rows accumulate silently | Use ReplacingMergeTree + FINAL |
| Missing FINAL on RMT query | Aggregates over-count during merge lag | Add FINAL to all analytical queries on RMT tables |
| ORDER BY from source schema | Slow queries; no block skipping | Derive ORDER BY from actual query filters |
| High-cardinality column first in ORDER BY | Poor index selectivity | Low cardinality first, high cardinality last |
| AggregateFunction column queried with sum() not sumMerge() | Silent numeric garbage | Always use `-Merge` combiners for AggregatingMergeTree |
| RMT version column that does not increase monotonically | Older version wins randomly | Use a timestamp that is always set to `now()` on update |

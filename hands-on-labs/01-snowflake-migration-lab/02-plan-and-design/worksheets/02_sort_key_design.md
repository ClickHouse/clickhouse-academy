# Worksheet 2: Sort Key Design (ORDER BY)

**Time estimate:** 20–25 minutes
**Reference:** [`docs/mergetree_guide.md`](../docs/mergetree_guide.md) — ORDER BY Design section

---

## Concept

ClickHouse's `ORDER BY` clause is not decorative. It defines the **primary index** — a sparse, block-level index that allows ClickHouse to skip irrelevant blocks of data when evaluating `WHERE` clauses. It also determines the physical sort order of data within parts, which affects compression.

**The wrong ORDER BY = slow queries + wasted storage.** A UUID-first ORDER BY means no block skipping for any analytical query (UUIDs are random; there is no sortable prefix). A date-first ORDER BY means queries filtering on date skip most of the table.

### Three rules for ORDER BY design

**Rule 1: Derive from query filters, not from the source schema.**
Look at the `WHERE`, `GROUP BY`, and `JOIN` columns across your most frequent queries. The most-filtered columns should probably appear in `ORDER BY` (Provided it is not too high cardinality). The source table's primary key (if any) is usually irrelevant.

**Rule 2: Low cardinality first, high cardinality last.**
ClickHouse's primary index has one entry per ~8192 rows (a *granule*). Low-cardinality columns (e.g., `toStartOfMonth(date)` = ~48 distinct values over 4 years) cluster many rows together — the index can skip entire granules. High-cardinality columns (e.g., `trip_id` = 50M distinct values) are unique per row — putting them first means the index cannot skip anything.

**Rule 3: For ReplacingMergeTree, end with the unique row identifier.**
The deduplication key is the full `ORDER BY` tuple. If `trip_id` is missing from `ORDER BY`, two different trips with the same `pickup_at` and no further columns would be treated as duplicates. Put `trip_id` last to ensure uniqueness without hurting index performance.

---

## Exercise: Query Workload Analysis

Before designing sort keys, analyze what columns the queries actually filter on.

The NYC Taxi lab has 7 representative queries. For each, identify the filter columns (WHERE, GROUP BY/ORDER BY on fact tables, JOIN keys):

| Query | Description | Filter / Group Columns | Most selective filter | 
|-------|-------------|----------------------|----------------------|
| Q1 | Hourly revenue by borough (DATE_TRUNC, DATEADD) | `pickup_at` (date range), `borough` (via dim join) | ? |
| Q2 | Rolling 7-day avg distance (window function) | `pickup_at::DATE` (GROUP BY — no WHERE filter) | ? |
| Q3 | Top 10 trips per zone (QUALIFY) | `pickup_at` (single day: yesterday), `pickup_borough` (PARTITION BY in window) | ? |
| Q4 | Driver ratings by zone (LATERAL FLATTEN / JSON) | JSON metadata fields (`driver` not null, `driver.rating` not null) | ? |
| Q5 | Surge pricing analysis (VARIANT colon-path) | `pickup_at` (date range) | ? |
| Q6 | Hourly aggregation (MERGE equivalent) | `hour_bucket`, `zone_id` | ? |
| Q7 | CDC stream lag measurement | `trip_id` (point lookup) | ? |

Fill in the "Most selective filter" column: which column, when filtered, eliminates the most rows?

---

## Exercise: Cardinality Estimation

For each candidate ORDER BY column, estimate its cardinality over the 4-year, 50M-row dataset:

| Column | Estimated Distinct Values | Cardinality Class |
|--------|--------------------------|------------------|
| `toStartOfMonth(pickup_at)` | ~48 (4 years × 12 months) | Very low |
| `toStartOfDay(pickup_at)` | ~1,460 (4 years × 365 days) | Low |
| `pickup_at` (DateTime64) | ~? | Medium-high |
| `pickup_location_id` | 265 (TLC zones) | Very low |
| `zone_id` | 265 | Very low |
| `hour_bucket` (by hour) | ~35,040 (4 years × 24 × 365) | Medium |
| `borough` | 6 | Extremely low |
| `payment_type` | 6 | Extremely low |
| `vendor_id` | 3 | Extremely low |
| `trip_id` (String UUID) | 50,000,000 | Extremely high |

Fill in the `pickup_at` estimated cardinality. (Hint: the trip producer inserts ~60 trips/minute. What is 60 trips/minute × 4 years in seconds?)

---

## Exercise: Sort Key Design

Using your query workload analysis and cardinality estimates, propose an `ORDER BY` for each table:

**Rules reminder:**
- Low cardinality first → most block-skipping
- Columns that appear in WHERE/GROUP BY of multiple queries → include them
- For ReplacingMergeTree tables → end with the unique row identifier
- Don't include columns that are never filtered on

| Table | Engine | Proposed ORDER BY | Reasoning |
|-------|--------|-------------------|-----------|
| `trips_raw` | ReplacingMergeTree(_synced_at) | `(?, ?)` | *Python migration script loads data; time-range queries filter on pickup_at; trip_id must be in ORDER BY as the RMT dedup key* |
| `fact_trips` | ReplacingMergeTree(updated_at) | `(?, ?, ?)` | *Q1-Q7 all filter on pickup_at; trip_id needed for RMT uniqueness* |
| `agg_hourly_zone_trips` | ReplacingMergeTree(updated_at) | `(?, ?)` | *Aggregation queries filter on hour_bucket and zone_id* |
| `dim_taxi_zones` | MergeTree | `(location_id)` | *(filled as example — dim tables are small, lookup by PK)* |

**Hints for `trips_raw`:**
- CDC lookups (`WHERE trip_id = ?`) need trip_id in ORDER BY for efficient point lookups
- Analytical range queries (`WHERE pickup_at BETWEEN ...`) benefit from pickup_at

**Hints for `fact_trips`:**
- All 7 analytical queries filter on `pickup_at` in some form
- `toStartOfMonth(pickup_at)` as the first column groups an entire month's data into adjacent blocks — this enables coarse-grained block skipping even without PARTITION BY
- `trip_id` must be last (RMT dedup key)

**Hints for `agg_hourly_zone_trips`:**
- Q6 groups by `hour_bucket` and `zone_id` — these are the only filter columns
- Which has lower cardinality?

---

## Reflection Questions

1. Why does `toStartOfMonth(pickup_at)` appear as the first column in `fact_trips` ORDER BY, rather than just `pickup_at`?

2. If Q7 does a point lookup by `trip_id` on `trips_raw`, should `trip_id` be first in `trips_raw`'s ORDER BY? Why or why not?

3. `dim_taxi_zones` has only 265 rows. Does the ORDER BY choice matter for this table? Explain.

---

<details>
<summary>▶ Answer Key — try the exercises first before expanding</summary>

### Query Workload Analysis

| Query | Most selective filter |
|-------|-----------------------|
| Q1 | `pickup_at` (date range eliminates 95%+ of rows before borough join) |
| Q2 | No WHERE filter — full table scan grouped by `pickup_at::DATE` |
| Q3 | `pickup_at` (single day — eliminates ~99.7% of 4-year dataset) |
| Q4 | JSON metadata presence check — not a sortable/skippable column |
| Q5 | `pickup_at` (date range) |
| Q6 | `hour_bucket` (specific time bucket) |
| Q7 | `trip_id` (UUID point lookup — unique per row) |

### Cardinality: pickup_at

60 trips/min × 60 min/hr × 24 hr/day × 365 days/yr × 4 years = ~126M possible minute-slots across 4 years. At second precision, even more. The 50M trips span ~2.1M distinct seconds → very high cardinality. Using `pickup_at` raw as the first ORDER BY column gives poor block skipping. `toStartOfMonth(pickup_at)` gives ~48 distinct values — much more useful as a first sort key.

### Sort Key Answers

| Table | Proposed ORDER BY | Reasoning |
|-------|-------------------|-----------|
| `trips_raw` | `(pickup_at, trip_id)` | Time-range analytical queries benefit from pickup_at first. trip_id for CDC point lookups (RMT uses it as dedup key if needed). |
| `fact_trips` | `(toStartOfMonth(pickup_at), pickup_at, trip_id)` | Month prefix clusters entire months together (block-level skipping). pickup_at narrows within month. trip_id ensures RMT uniqueness. Q1-Q7 all filter on pickup_at in some form — no query filters on trip_id alone on the fact table. **Production alternative:** `PARTITION BY toStartOfMonth(pickup_at)` + `ORDER BY (pickup_at, trip_id)` is equally valid — partitioning handles month-level pruning at the filesystem level, freeing the ORDER BY to focus purely on within-partition range scans. The lab uses the function-in-ORDER BY approach to keep the DDL simple, but both are production-grade patterns. |
| `agg_hourly_zone_trips` | `(hour_bucket, zone_id)` | Both columns appear in Q6. hour_bucket has medium cardinality (~35K); zone_id has low cardinality (265). In query patterns, `WHERE hour_bucket >= X` narrows more than `WHERE zone_id = Y`, but zone_id is a secondary filter. Both should be in ORDER BY. |

### Reflection Answers

1. `toStartOfMonth(pickup_at)` as the first column groups all trips in a calendar month into adjacent storage blocks. ClickHouse's sparse index can skip all blocks outside the requested date range at month granularity, before `pickup_at` narrows further within the month. If `pickup_at` were first alone, every granule would cover a mix of timestamps with no coarse structure — the index would still work but less efficiently for month-level range scans.

   > **Production note:** An equally valid design is `PARTITION BY toStartOfMonth(pickup_at)` + `ORDER BY (pickup_at, trip_id)`. `PARTITION BY` handles month-level pruning at the filesystem level (entire partition directories are skipped before any index is consulted), while `ORDER BY` focuses purely on within-partition range scans. The trade-off: partitioning creates more files and can hurt performance if partitions are too small (e.g., daily partitions on a sparse dataset). For 50M rows across 4 years (~1M rows/month), monthly partitioning is a safe choice. The lab keeps `toStartOfMonth` in ORDER BY to avoid introducing `PARTITION BY` as a separate concept, but SAs should know both patterns.

2. No. `trip_id` has extremely high cardinality (UUID = 50M distinct values). Putting it first means every block contains exactly one `trip_id` value range — the index is useless for anything except exact `trip_id` lookups. Since the vast majority of `trips_raw` queries are analytical (time-range scans), not CDC point lookups, `pickup_at` first is more valuable. The CDC lookup performance penalty for `trip_id` second is acceptable because CDC lookups are rare point-in-time operations, not the primary analytical workload.

3. `dim_taxi_zones` has 265 rows — one granule (8192 rows per granule). ClickHouse will read the entire table in a single granule regardless of ORDER BY. The choice does not matter for performance. `(location_id)` is still good practice for clarity and for join performance when ClickHouse uses a hash join and needs to look up rows by location_id.

</details>

---

## Transfer to migration-plan.md

Copy your ORDER BY decisions to Section 4 of `migration-plan.md` and check off:

```
- [ ] Sort key design: completed
```

# ClickHouse Migration Proficiency Assessment

**Partner Name:** _______________________________________________

**Company:** ___________________________________________________

**Date:** ______________________________________________________

**ClickHouse Solutions Architect:** _____________________________

**Lab Completion Date (Parts 1–3):** ____________________________

---

> **Instructions**
> - This is an open-book assessment — you may reference your `migration-plan.md`, benchmark results, and lab READMEs.
> - Section A: Write the letter of your answer (A, B, C, or D) on the `Your answer:` line.
> - Section B: Write your answers in the space provided. Aim for 3–6 sentences per sub-question.
> - When done, complete the submission checklist at the bottom and email to your SA.

---

## Section A — Multiple Choice (80 points)

*20 questions × 4 points each. No partial credit.*

---

### Part 1: Snowflake Workload Understanding

**Q1.** In the NYC Taxi lab, `trip_metadata` is stored as a `VARIANT` column in Snowflake containing nested JSON (driver rating, vehicle type, etc.). After migration to ClickHouse, which representation is recommended?

- A. `Map(String, String)` — enforces type safety on keys and values
- B. `String` column, with `JSONExtract*` functions applied at query time
- C. `Tuple(driver_rating Float32, vehicle_type String, …)` — pre-defined schema
- D. `JSON` — ClickHouse has a native `JSON` type; use it directly as a drop-in replacement for `VARIANT`

**Your answer:** ___

---

**Q2.** Your Snowflake pipeline uses this upsert pattern to keep `FACT_TRIPS` up to date when trips are corrected:

```sql
MERGE INTO ANALYTICS.FACT_TRIPS AS target
USING (SELECT * FROM STAGING.STG_TRIPS
       WHERE PICKUP_DATETIME > DATEADD('hour', -1, CURRENT_TIMESTAMP())) AS source
ON target.TRIP_ID = source.TRIP_ID
WHEN MATCHED THEN UPDATE SET
    TOTAL_AMOUNT = source.TOTAL_AMOUNT,
    UPDATED_AT   = source.UPDATED_AT
WHEN NOT MATCHED THEN INSERT VALUES (source.*)
```

Why can this not be directly ported to ClickHouse?

- A. ClickHouse supports `MERGE INTO` but requires the target table to use `ReplacingMergeTree`
- B. ClickHouse has no `MERGE INTO` statement — upserts are handled by `ReplacingMergeTree` engine combined with dbt's `delete_insert` incremental strategy
- C. ClickHouse's `MERGE INTO` requires a `PARTITION BY` clause to identify the target partition range
- D. `MERGE INTO` is only supported on ClickHouse's `AggregatingMergeTree` engine

**Your answer:** ___

---

**Q3.** Snowflake's `LATERAL FLATTEN(input => trip_tags)` expands an ARRAY column into one row per element. The ClickHouse equivalent syntax is:

- A. `UNNEST(trip_tags)`
- B. `ARRAY JOIN trip_tags`
- C. `EXPLODE(trip_tags)`
- D. `GROUP BY … WITH ROLLUP`

**Your answer:** ___

---

**Q4.** Your Snowflake pipeline uses:

```sql
MERGE INTO fact_trips USING staging
ON fact_trips.trip_id = staging.trip_id
WHEN MATCHED THEN UPDATE SET …
WHEN NOT MATCHED THEN INSERT …
```

What is the idiomatic ClickHouse approach for this upsert pattern?

- A. Use `ALTER TABLE … UPDATE` for matched rows and `INSERT INTO` for new rows
- B. Use `INSERT INTO fact_trips … ON CONFLICT DO UPDATE`
- C. Use `ReplacingMergeTree(updated_at)` and always INSERT the full row — ClickHouse deduplicates on compaction
- D. Use `MERGE` — ClickHouse supports MERGE DML syntax since v23.5

**Your answer:** ___

---

**Q5.** In the Snowflake lab (Part 1), a **Stream** captures CDC changes on `TRIPS_RAW` and a **Task** runs every hour to apply them to aggregation tables. In the ClickHouse architecture built in Part 3, what replaces this scheduled refresh pattern?

- A. A ClickPipes CDC connector on `trips_raw`
- B. A Refreshable Materialized View with `REFRESH EVERY 3 MINUTE`
- C. A standard Materialized View that triggers automatically on each INSERT
- D. A dbt scheduled run via GitHub Actions cron

**Your answer:** ___

---

### Part 2: Architecture & Design Decisions

**Q6.** You are designing a ClickHouse table to receive bulk INSERT batches from a migration script. The script may be retried mid-run, causing some rows to be re-inserted with the same primary key. Which engine makes retries idempotent?

- A. `MergeTree()` — duplicates pile up but can be filtered at query time
- B. `ReplacingMergeTree(_synced_at)` — later INSERT of the same primary key wins via the version column
- C. `AggregatingMergeTree()` — aggregates are commutative so duplicates cancel out
- D. `CollapsingMergeTree(sign)` — collapses duplicate rows using a +1/−1 sign column

**Your answer:** ___

---

**Q7.** `AggregatingMergeTree` is the correct engine choice when:

- A. You need to deduplicate rows by primary key using a version column
- B. You need fast point lookups by primary key with no aggregation
- C. You are pre-computing partial aggregates to be queried with `*Merge` combinators (e.g., `sumMerge`, `countMerge`)
- D. You need to retain the full history of row changes over time

**Your answer:** ___

---

**Q8.** Which of the following `ORDER BY` choices is most likely to **hurt** analytical query performance on `fact_trips`?

- A. `ORDER BY (pickup_at)`
- B. `ORDER BY (toStartOfMonth(pickup_at), pickup_at, trip_id)`
- C. `ORDER BY (trip_id, pickup_at)`
- D. `ORDER BY (pickup_location_id, pickup_at)`

**Your answer:** ___

---

**Q9.** A query on `analytics.fact_trips` returns duplicate `trip_id` values. You add `FINAL` to the query and duplicates disappear. What is the trade-off of using `FINAL`?

- A. `FINAL` permanently deduplicates the table but requires a full table lock during execution
- B. `FINAL` forces an in-memory merge at query time, increasing latency — without it, duplicates can reappear until background merges complete
- C. `FINAL` only deduplicates within a single data part, not across parts from different INSERTs
- D. `FINAL` is only supported on `AggregatingMergeTree` tables, not `ReplacingMergeTree`

**Your answer:** ___

---

**Q10.** In the Part 3 dbt pipeline, `fact_trips` uses the `delete_insert` incremental strategy. What does this strategy do on each dbt run?

- A. Runs `DELETE FROM fact_trips WHERE …` to remove the overlapping range, then `INSERT INTO fact_trips SELECT … WHERE …` with fresh rows
- B. Uses `REPLACE INTO` which atomically deletes matching rows and inserts the new version in one statement
- C. Drops and recreates the entire table, then re-inserts all rows from scratch
- D. Marks stale rows with a `_deleted = true` flag and inserts the new version alongside them

**Your answer:** ___

---

**Q11.** `fact_trips` has `ORDER BY (toStartOfMonth(pickup_at), pickup_at, trip_id)`. Which query benefits most from this sort key through granule-level data skipping?

- A. `SELECT count() FROM fact_trips WHERE trip_id = 'abc-123'`
- B. `SELECT sum(fare_amount_usd) FROM fact_trips WHERE pickup_at BETWEEN '2025-01-01' AND '2025-03-31'`
- C. `SELECT avg(tip_amount_usd) FROM fact_trips WHERE vendor_id = 2`
- D. `SELECT * FROM fact_trips ORDER BY total_amount_usd DESC LIMIT 10`

**Your answer:** ___

---

**Q12.** On `trips_raw`, the `pickup_at` column is a `DateTime` that increases roughly monotonically over time. Which storage codec best compresses it?

- A. `CODEC(ZSTD(1))` — general-purpose lossless compression
- B. `CODEC(LZ4)` — optimised for fast decompression on hot queries
- C. `CODEC(Delta, ZSTD(1))` — encodes the small differences between consecutive values, then compresses
- D. `CODEC(Gorilla, ZSTD(1))` — designed for floating-point time series data

**Your answer:** ___

---

### Part 3: Migration Execution & Validation

**Q13.** Your Snowflake pipeline uses `HOURLY_AGG_TASK`, a Scheduled Task that runs a `MERGE INTO AGG_HOURLY_ZONE_TRIPS` statement every hour. What is the recommended ClickHouse equivalent for recalculating hourly aggregates on a schedule?

- A. A standard Materialized View on `trips_raw` — it updates `agg_hourly_zone_trips` automatically on every INSERT
- B. A Refreshable Materialized View with `REFRESH EVERY 1 HOUR` — it re-executes a full SELECT and atomically replaces the result set on each cycle
- C. An AggregatingMergeTree table — partial aggregate states merge automatically in the background without a scheduler
- D. A dbt `table` model with a `+post-hook: "OPTIMIZE TABLE agg_hourly_zone_trips FINAL"` to trigger compaction after each run

**Your answer:** ___

---

**Q14.** Your Snowflake `FACT_TRIPS` table is clustered on `PICKUP_AT::DATE`. You migrate to ClickHouse with `ORDER BY (toStartOfMonth(pickup_at), pickup_at, trip_id)`. A colleague asks why you added `toStartOfMonth(pickup_at)` as the leading key rather than using `pickup_at` alone. The correct explanation is:

- A. ClickHouse builds a dense index with one entry per distinct value in the first `ORDER BY` column — a raw `DateTime` with millions of distinct timestamps would create an index too large to fit in RAM
- B. ClickHouse's sparse primary index stores one entry per 8,192 rows; a month prefix enables the index to skip entire months of data on monthly aggregation queries, while `pickup_at` alone provides only row-level granularity within each 8,192-row block
- C. `toStartOfMonth()` is required by `ReplacingMergeTree` — version column deduplication only works correctly within month boundaries
- D. A raw `DateTime` column cannot be the first key in `ORDER BY` in ClickHouse — it must be wrapped in a date-truncation function

**Your answer:** ___

---

**Q15.** Your first benchmark run shows ClickHouse Q1 (date-range aggregation) at 1.8 seconds. You run the exact same query immediately afterward and it completes in 0.4 seconds. The most likely explanation is:

- A. ClickHouse background merges completed between runs, improving sort order and reducing scan size
- B. The OS page cache warmed the compressed data files on the first run; the second run is served from RAM
- C. The `mv_hourly_revenue` Refreshable Materialized View refreshed between the two runs
- D. ClickHouse's query result cache (`use_query_cache`) is enabled by default and returned the pre-computed result on the second execution

**Your answer:** ___

---

**Q16.** You insert 100 rows into `trips_raw` where all 100 share the same `trip_id` (intentional duplicate test). You immediately run `SELECT count() FROM default.trips_raw` and see 100, not 1. Why?

- A. `ReplacingMergeTree` requires a `SELECT … FINAL` even after compaction has occurred
- B. The rows have different `_synced_at` values so ClickHouse considers them distinct and keeps all 100
- C. Deduplication in `ReplacingMergeTree` happens during background part merges, which are asynchronous — the parts have not merged yet
- D. You must run `OPTIMIZE TABLE trips_raw FINAL` before row counts become accurate

**Your answer:** ___

---

**Q17.** A customer needs to migrate 500 million rows from Snowflake to ClickHouse Cloud as quickly as possible. Which approach gives the highest throughput and is most commonly used in production migrations?

- A. A Python script reading from Snowflake's cursor API in batches and writing via `clickhouse-connect`
- B. Export Snowflake data to S3 as Parquet files using `COPY INTO @stage`, then ingest with `INSERT INTO ... SELECT * FROM s3('s3://...', 'Parquet')`
- C. Use Snowflake's JDBC driver to stream rows directly to ClickHouse's HTTP interface without intermediate storage
- D. Use `dbt run --full-refresh` pointed at both Snowflake and ClickHouse simultaneously to synchronize the tables

**Your answer:** ___

---

**Q18.** A data analyst queries `analytics.fact_trips` (no `FINAL`) and gets 49,998,201 rows. Ten minutes later, with no new inserts, the same query returns 49,998,198 rows. `SELECT COUNT() FROM analytics.fact_trips FINAL` consistently returns 49,998,198. What explains the initial higher count?

- A. ClickHouse Cloud replicates data across availability zones — the two queries hit different replicas with different replication lag
- B. A background part merge completed between the two queries, deduplicating 3 rows that shared the same `trip_id` across separate unmerged parts — the first query counted all copies before the merge ran
- C. The OS page cache returned stale results from the first query's scan of an older data snapshot
- D. The `mv_hourly_revenue` Refreshable Materialized View deleted source rows from `fact_trips` during its refresh cycle

**Your answer:** ___

---

**Q19.** The CH — Capabilities Showcase dashboard includes a chart comparing `uniqHLL12(trip_id)` with `uniq(trip_id)`. What is the key trade-off between them?

- A. `uniqHLL12` is exact but slower; `uniq` uses HyperLogLog and is an approximation
- B. `uniqHLL12` uses HyperLogLog (~1–2% error, ~16 KB memory); `uniq` is more precise but uses significantly more memory
- C. Both use HyperLogLog, but `uniqHLL12` uses 12 registers while `uniq` uses 64 — making `uniq` more accurate
- D. `uniqHLL12` only works on `String` columns; `uniq` works on any data type

**Your answer:** ___

---

**Q20.** The CH — Capabilities Showcase dashboard uses `dictGet('analytics.taxi_zones_dict', 'zone', toUInt16(pickup_location_id))` instead of a `JOIN` on the zone dimension table. Why does this outperform a JOIN?

- A. ClickHouse loads dictionaries into GPU memory for hardware-accelerated lookups
- B. The dictionary is materialised as a hash table in RAM — each lookup is O(1) with no disk I/O, unlike a JOIN that probes data parts on disk
- C. `dictGet` is processed by ClickHouse's vectorised execution engine, while JOIN operations cannot be vectorised
- D. The dictionary is stored in a ZooKeeper node, making it available across all replicas without replication lag

**Your answer:** ___

---

## Section B — Open Questions (20 points)

*4 questions × 5 points each. Write your answers in the space provided.*
*This is open-book — reasoning and depth of explanation matter more than exact wording.*

---

**Open Q1 — Engine Selection for a New Scenario** *(5 pts)*

A prospective customer has a `sessions` table that tracks user login sessions. Each row has `(session_id UUID, user_id UInt64, started_at DateTime, ended_at Nullable(DateTime), event_type Enum('active','completed','expired'))`. Sessions are frequently updated — a session recorded as `active` will later be updated to `completed` or `expired`. Multiple services write updates simultaneously.

**1a.** Which MergeTree engine family would you recommend, and why? *(2 pts)*

*Your answer:*

---

**1b.** What would you use as the `ORDER BY` for this table, and why? *(2 pts)*

*Your answer:*

---

**1c.** What query-time behaviour must you warn the customer about, and how do you address it? *(1 pt)*

*Your answer:*

---

**Open Q2 — SQL Translation Challenge** *(5 pts)*

Translate the following Snowflake SQL to valid ClickHouse SQL. After your translation, briefly explain each change you made and why.

> **Constraint:** Write the equivalent **without `QUALIFY`** — use a subquery instead. This lab treats `QUALIFY` as a portability gap and uses the subquery form throughout, so that the pattern works identically across all SQL engines.

```sql
-- Snowflake: top-earning vendor per borough in the last 30 days
SELECT
    tz.borough,
    t.vendor_id,
    SUM(t.fare_amount)                                                      AS total_fare,
    ROW_NUMBER() OVER (PARTITION BY tz.borough ORDER BY SUM(t.fare_amount) DESC) AS rank
FROM trips_raw t
JOIN taxi_zones tz ON t.pickup_location_id = tz.location_id
WHERE t.pickup_datetime >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND t.fare_amount > 0
GROUP BY tz.borough, t.vendor_id
QUALIFY rank = 1
```

*Your ClickHouse translation:*

```sql
-- ClickHouse translation:

```

*Explanation of changes:*

---

**Open Q3 — Reflection: Biggest Conceptual Shift** *(5 pts)*

Based on completing Parts 1–3, describe **one concept** that was most different from how you expected ClickHouse to work, compared to Snowflake.

**3a.** What did you expect, and what does ClickHouse actually do? *(2 pts)*

*Your answer:*

---

**3b.** Why is ClickHouse designed this way — what problem does this design solve? *(2 pts)*

*Your answer:*

---

**3c.** How would you explain this difference to a customer evaluating ClickHouse for the first time? *(1 pt)*

*Your answer:*

---

**Open Q4 — Real-World Application** *(5 pts)*

Identify a customer opportunity or internal use case — real or hypothetical — where the Snowflake → ClickHouse migration pattern from this lab could apply.

**4a.** Describe the customer's workload: what data, what query patterns, and what is their pain point with Snowflake? *(1 pt)*

*Your answer:*

---

**4b.** Which ClickHouse engine(s) would you recommend, and what would the key `ORDER BY` column(s) be? Justify your choices based on the query patterns. *(2 pts)*

*Your answer:*

---

**4c.** Identify one specific technical risk or challenge in this migration and describe how you would mitigate it. *(2 pts)*

*Your answer:*

---

## Submission Checklist

Before emailing your completed assessment to your ClickHouse SA, confirm:

- [ ] All 20 MCQ answers filled in (A, B, C, or D on each `Your answer:` line)
- [ ] All 4 open questions answered with substantive responses
- [ ] `migration-plan.md` from Part 2 attached (or pasted inline)
- [ ] `benchmark_results_*.csv` from Part 3 attached
- [ ] Your name, company, and SA name are filled in at the top of this file

**Submit to:** your ClickHouse Solutions Architect via email.
**Subject line:** `[ClickHouse Migration Badge] <Your Name> — Assessment Submission`

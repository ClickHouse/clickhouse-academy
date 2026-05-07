# Worksheet 3: Schema Translation

**Time estimate:** 20–25 minutes
**Reference:** [`docs/snowflake_vs_clickhouse.md`](../docs/snowflake_vs_clickhouse.md) — Section 2 (SQL Dialect Gaps)

---

## Concept

Type mapping and function translation are the most mechanical part of migration, but also the most error-prone if done carelessly. Snowflake and ClickHouse have different type systems with different semantics, and using the wrong type can cause silent precision loss, excessive storage, or broken query logic.

**Key principles:**

1. **Be explicit about precision.** Snowflake's `TIMESTAMP_NTZ` has nanosecond precision. ClickHouse's `DateTime` has only second precision — do not use it for version columns. Use `DateTime64(3, 'UTC')` for millisecond precision (matching most real-world requirements) or `DateTime64(9, 'UTC')` for nanoseconds. **This matters for correctness:** if a `ReplacingMergeTree` version column has only second precision, two updates arriving within the same second are non-deterministic — ClickHouse cannot determine which is newer.

2. **Use the smallest correct integer type.** Snowflake's `INTEGER` is `NUMBER(38, 0)` — 38-digit fixed precision stored as a 128-bit value. ClickHouse has fixed-width integers: `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`. Choosing `UInt8` for `vendor_id` (values 1–3) saves 7 bytes per row vs `Int64`. At 50M rows, that is 350MB.

3. **VARIANT → String.** ClickHouse has a native `JSON` type (available in v25.3+ as production-stable), but it is designed for truly dynamic schemas where the field names and structure are unknown at table creation time. For `trip_metadata` in this lab, the structure is known (`driver.rating`, `app.surge_multiplier`, etc.) — the better approach is to pre-flatten into typed columns during migration, or store as `String` and use `JSONExtract*` at query time. Use the `JSON` type when you genuinely cannot predict the schema: e.g., ingesting arbitrary customer event payloads where every event has different fields.

4. **Float precision.** Snowflake's `FLOAT` maps to `Float64` in ClickHouse. For monetary amounts where exact decimal arithmetic is required, use `Decimal(18, 2)` — but for this lab, `Float64` is sufficient to match the source.

5. **LowCardinality() — ClickHouse-only optimization.** Wrapping a type in `LowCardinality(String)` (or `LowCardinality(UInt8)`, etc.) tells ClickHouse to use a dictionary encoding for that column — values are stored as integer references to a dictionary rather than repeated strings. This typically gives 2–5x compression improvement and faster GROUP BY on string columns with fewer than ~10,000 distinct values. Snowflake has no equivalent; it handles this automatically. Good candidates in this lab: `pickup_borough` (6 values), `payment_type` (6 values), `vehicle_type`, `vendor_name`.

---

## Exercise 1: Type Mapping for TRIPS_RAW

Map each column from `NYC_TAXI_DB.RAW.TRIPS_RAW` to its ClickHouse type. Fill in the `?` cells:

| Snowflake Column | Snowflake Type | ClickHouse Type | Notes |
|------------------|---------------|-----------------|-------|
| `TRIP_ID` | `VARCHAR(36)` | `String` | *(ClickHouse has a native [`UUID`](https://clickhouse.com/docs/sql-reference/data-types/uuid) type, but `String` is idiomatic when migrating from VARCHAR(36): it requires no casting, supports all string functions, and avoids UUID parsing overhead on insert)* |
| `PICKUP_DATETIME` | `TIMESTAMP_NTZ(9)` | ? | *Hint: what precision does DateTime64 support? What timezone?* |
| `DROPOFF_DATETIME` | `TIMESTAMP_NTZ(9)` | ? | *Same as PICKUP_DATETIME* |
| `PICKUP_LOCATION_ID` | `INTEGER` | ? | *Values 1–265. What is the smallest correct unsigned integer type?* |
| `DROPOFF_LOCATION_ID` | `INTEGER` | ? | *Same as PICKUP_LOCATION_ID* |
| `VENDOR_ID` | `INTEGER` | ? | *Values 1–3. What is the smallest correct unsigned integer type?* |
| `PASSENGER_COUNT` | `INTEGER` | ? | *Values 1–6 typically. What type?* |
| `TRIP_DISTANCE` | `FLOAT` | ? | *Decimal miles — precision matters somewhat but not critically* |
| `FARE_AMOUNT` | `FLOAT` | ? | *Monetary. Float64 is acceptable for this lab.* |
| `TIP_AMOUNT` | `FLOAT` | ? | *Same as FARE_AMOUNT* |
| `TOTAL_AMOUNT` | `FLOAT` | ? | *Same as FARE_AMOUNT* |
| `PAYMENT_TYPE_ID` | `INTEGER` | ? | *Values 1–6. What type?* |
| `TRIP_METADATA` | `VARIANT` | ? | *JSON blob — the key migration challenge* |
| `CREATED_AT` | `TIMESTAMP_NTZ(9)` | ? | *Row creation timestamp* |

---

## Exercise 2: Type Mapping for FACT_TRIPS

`FACT_TRIPS` adds computed/derived columns that were added by the dbt pipeline:

| Snowflake Column | Snowflake Type | ClickHouse Type | Notes |
|------------------|---------------|-----------------|-------|
| `TRIP_ID` | `VARCHAR(36)` | `String` | *(filled)* |
| `PICKUP_AT` | `TIMESTAMP_NTZ(9)` | ? | |
| `DROPOFF_AT` | `TIMESTAMP_NTZ(9)` | ? | |
| `PICKUP_LOCATION_ID` | `INTEGER` | ? | |
| `ZONE_ID` | `INTEGER` | ? | *Foreign key to dim_taxi_zones; same cardinality as location IDs* |
| `FARE_AMOUNT` | `FLOAT` | ? | |
| `TRIP_DISTANCE` | `FLOAT` | ? | |
| `DRIVER_RATING` | `FLOAT` | ? | *Values 1.0–5.0; can be NULL (no rating)* |
| `PAYMENT_TYPE_ID` | `INTEGER` | ? | |
| `VENDOR_ID` | `INTEGER` | ? | |
| `UPDATED_AT` | `TIMESTAMP_NTZ(9)` | ? | *Version column for ReplacingMergeTree — precision matters* |

**Note on nullable columns:** In ClickHouse, `Nullable(Float64)` has a slight performance overhead compared to `Float64`. For `driver_rating`, which is frequently NULL, you have two options:
- `Nullable(Float32)` — explicit null semantics; slower on aggregations due to null-tracking overhead (a separate bitmask column is stored alongside the data)
- `Float32` with `nan` or `-1.0` as sentinel value — faster but less conventional

For the lab, use `Nullable(Float32)` for correctness.

---

## Exercise 3: Function Translation

Translate each Snowflake expression to its ClickHouse equivalent. These come directly from Q1–Q7 in `01-setup-snowflake/queries/`.

| Snowflake Expression | Query | ClickHouse Equivalent |
|----------------------|-------|----------------------|
| `DATE_TRUNC('hour', pickup_at)` | Q1 | ? |
| `DATEADD('day', -7, CURRENT_DATE)` | Q1, Q3 | ? |
| `DATEDIFF('minute', pickup_at, dropoff_at)` | Q4 | ? |
| `TRIP_METADATA:driver.rating::FLOAT` | Q4, Q5 | ? |
| `TRIP_METADATA:app.surge_multiplier::FLOAT` | Q5 | ? |
| `QUALIFY ROW_NUMBER() OVER (PARTITION BY pickup_location_id ORDER BY fare_amount DESC) <= 10` | Q3 | ? — treated as a dialect gap in this lab; requires structural rewrite |
| `MERGE INTO fact_trips t USING staging s ON t.trip_id = s.trip_id WHEN MATCHED THEN UPDATE ...` | Q6 | ? — requires structural rewrite *(Hint: think about the engine and dbt strategy you chose in Worksheets 1 & 2)* |
| `SELECT METADATA$ACTION, METADATA$ISUPDATE FROM trips_cdc_stream` | Q7 | ? — no equivalent needed; live writes go directly to ClickHouse post-cutover; what pattern gives you current state? |

**Hints:**
- `DATE_TRUNC('hour', ...)` → look at `toStartOfHour` family in ClickHouse docs
- `DATEADD('day', -7, ...)` → ClickHouse uses `today() - 7` for dates, or `now() - INTERVAL 7 DAY`
- `DATEDIFF('minute', t1, t2)` → ClickHouse has `dateDiff` (lowercase d)
- `TRIP_METADATA:driver.rating::FLOAT` → nested path requires two-level `JSONExtractFloat`
- QUALIFY → for this lab, treated as a dialect gap; wrap in a subquery: keep the window function, add `WHERE` outside (this rewrite is portable across all SQL engines)

---

## Exercise 4: Non-Obvious Translation Decisions

For each decision below, write your reasoning (not just the answer):

**Decision 1:** `TRIP_METADATA` is `VARIANT` in Snowflake. You have three options in ClickHouse:
- `String` — store raw JSON; use `JSONExtract*` at query time
- `Map(String, String)` — structured key-value; loses nested structures
- `Tuple(...)` — fixed schema; breaks if JSON structure varies

Which do you choose and why?

*Your answer:*

---

**Decision 2:** `FARE_AMOUNT` is `FLOAT` in Snowflake. In ClickHouse, you could use `Float64`, `Float32`, or `Decimal(18, 2)`. The lab has 50M rows.

Which do you choose and why? (Consider: precision needs, storage cost, and whether fare amounts in this workload require exact decimal arithmetic.)

*Your answer:*

---

**Decision 3:** `PICKUP_LOCATION_ID` is `INTEGER` in Snowflake (values 1–265). In ClickHouse, you could use `Int32`, `Int16`, or `UInt16`.

Calculate the storage savings of using `UInt16` instead of `Int32` at 50M rows:

*Your calculation:*

---

<details>
<summary>▶ Answer Key — try the exercises first before expanding</summary>

### Exercise 1: TRIPS_RAW Types

| Column | ClickHouse Type | Reasoning |
|--------|----------------|-----------|
| `PICKUP_DATETIME` | `DateTime64(3, 'UTC')` | Millisecond precision is sufficient; `'UTC'` makes timezone explicit. Nanosecond (`9`) is overkill for trip timestamps. |
| `DROPOFF_DATETIME` | `DateTime64(3, 'UTC')` | Same. |
| `PICKUP_LOCATION_ID` | `UInt16` | Values 1–265. `UInt8` max is 255 — too small. `UInt16` max is 65535 — correct and half the size of `Int32`. |
| `DROPOFF_LOCATION_ID` | `UInt16` | Same. |
| `VENDOR_ID` | `UInt8` | Values 1–3. `UInt8` max is 255 — correct. 1 byte per row vs 4 for Int32. |
| `PASSENGER_COUNT` | `UInt8` | Values 1–6. `UInt8` correct. |
| `TRIP_DISTANCE` | `Float64` | Float distance; no exact decimal requirement. Matches Snowflake FLOAT. |
| `FARE_AMOUNT` | `Float64` | See Exercise 4 Decision 2. |
| `TIP_AMOUNT` | `Float64` | Same. |
| `TOTAL_AMOUNT` | `Float64` | Same. |
| `PAYMENT_TYPE_ID` | `UInt8` | Values 1–6. |
| `TRIP_METADATA` | `String` | See Exercise 4 Decision 1. |
| `CREATED_AT` | `DateTime64(3, 'UTC')` | Row creation timestamp — milliseconds sufficient. |

### Exercise 2: FACT_TRIPS Types

| Column | ClickHouse Type |
|--------|----------------|
| `PICKUP_AT` | `DateTime64(3, 'UTC')` |
| `DROPOFF_AT` | `DateTime64(3, 'UTC')` |
| `PICKUP_LOCATION_ID` | `UInt16` |
| `ZONE_ID` | `UInt16` |
| `FARE_AMOUNT` | `Float64` |
| `TRIP_DISTANCE` | `Float64` |
| `DRIVER_RATING` | `Nullable(Float32)` |
| `PAYMENT_TYPE_ID` | `UInt8` |
| `VENDOR_ID` | `UInt8` |
| `UPDATED_AT` | `DateTime64(3, 'UTC')` — must be DateTime64 not DateTime; second precision could cause two updates in the same second to be non-deterministic |

### Exercise 3: Function Translations

| Snowflake Expression | ClickHouse Equivalent |
|----------------------|-----------------------|
| `DATE_TRUNC('hour', pickup_at)` | `toStartOfHour(pickup_at)` |
| `DATEADD('day', -7, CURRENT_DATE)` | `today() - 7` or `addDays(today(), -7)` |
| `DATEDIFF('minute', pickup_at, dropoff_at)` | `dateDiff('minute', pickup_at, dropoff_at)` |
| `TRIP_METADATA:driver.rating::FLOAT` | `JSONExtractFloat(trip_metadata, 'driver', 'rating')` |
| `TRIP_METADATA:app.surge_multiplier::FLOAT` | `JSONExtractFloat(trip_metadata, 'app', 'surge_multiplier')` |
| `QUALIFY ROW_NUMBER() OVER (...) <= 10` | `SELECT ... FROM (SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM ...) WHERE rn <= 10` |
| `MERGE INTO ... WHEN MATCHED THEN UPDATE` | dbt `delete_insert` incremental strategy — no direct SQL equivalent |
| `SELECT METADATA$ACTION FROM stream` | No equivalent needed — live writes go directly to ClickHouse post-cutover; query the target table with `FINAL` for current deduplicated state |

### Exercise 4: Decisions

**Decision 1 — TRIP_METADATA → String**
Choose `String`. `Map(String, String)` loses nested structures (the rating is nested under `driver.rating`, not a flat key). `Tuple(...)` requires a fixed schema — if the JSON structure varies across trips, the Tuple definition fails or requires frequent schema migrations. `String` preserves the raw JSON exactly, and `JSONExtract*` handles any path at query time. The trade-off is that extraction is slower than native column access, but for VARIANT columns that are queried infrequently (only in Q4, Q5), this is acceptable.

**Decision 2 — FARE_AMOUNT → Float64**
Use `Float64`. `Float32` has ~7 significant digits of precision — a $10,000.00 fare would be stored as $9,999.999... (rounding at 7 digits), which is wrong for financial reporting. `Decimal(18, 2)` gives exact precision but is slower on aggregations. For a lab environment where we're demonstrating analytics performance, `Float64` (15–16 significant digits) is the pragmatic choice. In a production financial system, `Decimal(18, 2)` would be correct.

**Decision 3 — UInt16 vs Int32 storage savings**
- `Int32`: 4 bytes per row × 50,000,000 rows = 200,000,000 bytes = ~190 MB
- `UInt16`: 2 bytes per row × 50,000,000 rows = 100,000,000 bytes = ~95 MB
- Savings: ~95 MB per column before compression
- ClickHouse compression (LZ4 default) compresses sorted integer columns at ~5-10x for low-cardinality values → actual storage might be 10-20 MB per column regardless
- But: two `UInt16` columns (`pickup_location_id` + `dropoff_location_id`) on 50M rows saves ~190 MB raw storage and improves compression since small integers compress better

</details>

---

## Transfer to migration-plan.md

Copy your type decisions and any non-obvious translation notes to Section 5 of `migration-plan.md` and check off:

```
- [ ] Schema translation: completed
```

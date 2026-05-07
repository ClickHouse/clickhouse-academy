# Superset Datasets, Charts & Dashboards — Manual Setup Guide

This guide walks through the full manual setup of all ClickHouse datasets, charts, and dashboards in Superset. Follow this to understand what each visualization does and how it is built.

> **Want to skip ahead?** Run the import script to have everything created automatically:
> ```bash
> source .env && source .clickhouse_state
> bash superset/add_clickhouse_connection.sh
> ```
> The script creates the connection, imports all 7 datasets, 18 charts, and 4 dashboards in one shot. Use this guide as a reference or to rebuild individual pieces.

> **Important — placeholder credentials in `superset/dashboards/dashboard_export_*.zip`.**
>
> The committed dashboard ZIP has its `databases/*.yaml` redacted to placeholders:
>
> ```
> sqlalchemy_uri: clickhousedb://default:XXXXXXXXXX@your-instance.clickhouse.cloud:8443/analytics?secure=true
> ```
>
> - **Auto-import (`add_clickhouse_connection.sh`)** — works as-is. The script rewrites `sqlalchemy_uri` inside the ZIP using `${CLICKHOUSE_HOST}` / `${CLICKHOUSE_USER}` / `${CLICKHOUSE_PASSWORD}` from your `.env` *before* posting to the import endpoint, so the placeholder host never reaches Superset.
> - **Manual import via Superset UI** — the imported database will be created with `your-instance.clickhouse.cloud` and won't connect. After import, go to **Settings → Database Connections → Edit** the entry and replace `sqlalchemy_uri` with your real ClickHouse Cloud URI (e.g. `clickhousedb://default:<PASSWORD>@<your-host>.clickhouse.cloud:8443/analytics?secure=true`).
> - **Re-exporting your own dashboards** — Superset bakes your real ClickHouse host into the export. Before committing, redact the host back to `your-instance.clickhouse.cloud` so your service identifier doesn't leak into git history.

**Prerequisites:** The analytics layer is populated (`dbt run` done in Step 7.3) and the dictionary exists (`scripts/04_create_dictionary.sql` done in Step 7.4).

---

## Step 0 — Register the ClickHouse Connection

1. Log in to Superset at [http://localhost:8088](http://localhost:8088) (admin / admin).
2. Go to **Settings → Database Connections**.
3. Click **+ Database**.
4. Select **ClickHouse Connect** from the list.
5. Fill in:

   | Field | Value |
   |-------|-------|
   | Display Name | `NYC Taxi — ClickHouse Cloud` |
   | Host | your ClickHouse Cloud hostname (from `.clickhouse_state`) |
   | Port | `8443` |
   | Database | `analytics` |
   | Username | `default` |
   | Password | your ClickHouse Cloud password |
   | SSL | enabled |

6. Click **Test Connection** — confirm the green success banner.
7. Click **Connect**.

---

## Part 1 — Create Datasets

### Dataset 1 — `fact_trips` (table dataset)

Used by: Operations Command Center, Executive Weekly Report, Driver Quality Analytics, Capabilities Showcase

1. Go to **Datasets → + Dataset**.
2. Set **Database** = `NYC Taxi — ClickHouse Cloud`, **Schema** = `analytics`, **Table** = `fact_trips`.
3. Click **Add Dataset and Create Chart** → then navigate away — the dataset is saved.

### Dataset 2 — `agg_hourly_zone_trips` (table dataset)

Used by: Operations Command Center, Executive Weekly Report

1. Go to **Datasets → + Dataset**.
2. Set **Database** = `NYC Taxi — ClickHouse Cloud`, **Schema** = `analytics`, **Table** = `agg_hourly_zone_trips`.
3. Click **Save**.

### Dataset 3 — `CH Approx Unique Trips (uniqHLL12)` (virtual)

Used by: Capabilities Showcase — demonstrates `uniqHLL12()` approximate counting vs exact `uniq()`.

1. Go to **Datasets → + Dataset**.
2. Click **Switch to SQL Lab** (or select the **Virtual** tab).
3. Set **Database** = `NYC Taxi — ClickHouse Cloud`.
4. Paste SQL:

```sql
SELECT
  toDate(pickup_at)                                                     AS day,
  uniq(trip_id)                                                         AS exact_unique_trips,
  uniqHLL12(trip_id)                                                    AS approx_unique_trips,
  round(
    abs(uniq(trip_id) - uniqHLL12(trip_id)) / uniq(trip_id) * 100, 2
  )                                                                     AS pct_error
FROM analytics.fact_trips FINAL
WHERE pickup_at >= today() - INTERVAL 30 DAY
GROUP BY day
ORDER BY day
```

5. Name it **`CH Approx Unique Trips (uniqHLL12)`** and click **Save**.

### Dataset 4 — `CH Cohort Retention` (virtual)

Used by: Capabilities Showcase — demonstrates window functions (`AVG(...) OVER (...)`) for rolling revenue by borough.

1. Go to **Datasets → + Dataset → Virtual**.
2. Set **Database** = `NYC Taxi — ClickHouse Cloud`.
3. Paste SQL:

```sql
SELECT
  week,
  pickup_borough,
  trips,
  revenue,
  round(avg(revenue) OVER (
    PARTITION BY pickup_borough
    ORDER BY week
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  ), 2) AS rolling_4wk_avg_revenue
FROM (
  SELECT
    toStartOfWeek(pickup_at)       AS week,
    pickup_borough,
    count()                        AS trips,
    round(sum(fare_amount_usd), 2) AS revenue
  FROM analytics.fact_trips FINAL
  GROUP BY week, pickup_borough
)
ORDER BY week DESC, revenue DESC
LIMIT 100
```

4. Name it **`CH Cohort Retention`** and click **Save**.

### Dataset 5 — `CH Fare Percentiles (quantileTDigest)` (virtual)

Used by: Capabilities Showcase — demonstrates `quantileTDigest()` as a ClickHouse-native percentile function.

1. Go to **Datasets → + Dataset → Virtual**.
2. Set **Database** = `NYC Taxi — ClickHouse Cloud`.
3. Paste SQL:

```sql
SELECT
  vendor_name,
  quantileTDigest(0.5)(fare_amount_usd)  AS p50_fare,
  quantileTDigest(0.95)(fare_amount_usd) AS p95_fare,
  quantileTDigest(0.99)(fare_amount_usd) AS p99_fare,
  count()                                AS trip_count
FROM analytics.fact_trips FINAL
GROUP BY vendor_name
ORDER BY vendor_name
```

4. Name it **`CH Fare Percentiles (quantileTDigest)`** and click **Save**.

### Dataset 6 — `CH Sampling Demo` (virtual)

Used by: Capabilities Showcase — demonstrates `rand() % N` sampling vs a full scan, comparing accuracy.

1. Go to **Datasets → + Dataset → Virtual**.
2. Set **Database** = `NYC Taxi — ClickHouse Cloud`.
3. Paste SQL:

```sql
SELECT
  'Full Scan'              AS method,
  count()                  AS trip_count,
  round(avg(fare_amount_usd), 4) AS avg_fare
FROM analytics.fact_trips FINAL
UNION ALL
SELECT
  '~10% (rand() % 10 = 0)' AS method,
  count() * 10              AS trip_count_est,
  round(avg(fare_amount_usd), 4) AS avg_fare
FROM analytics.fact_trips FINAL
WHERE rand() % 10 = 0
```

4. Name it **`CH Sampling Demo`** and click **Save**.

### Dataset 7 — `CH Zone Dict Lookup` (virtual)

Used by: Capabilities Showcase — demonstrates `dictGet()` for zero-JOIN dimension enrichment.

> **Requires:** the dictionary `analytics.taxi_zones_dict` from Step 7.4 (`scripts/04_create_dictionary.sql`).

1. Go to **Datasets → + Dataset → Virtual**.
2. Set **Database** = `NYC Taxi — ClickHouse Cloud`.
3. Paste SQL:

```sql
SELECT
  dictGet('analytics.taxi_zones_dict', 'borough', toUInt16(pickup_location_id)) AS borough,
  dictGet('analytics.taxi_zones_dict', 'zone',    toUInt16(pickup_location_id)) AS zone,
  count()                                                                        AS trips,
  round(avg(fare_amount_usd), 2)                                                 AS avg_fare
FROM default.trips_raw
WHERE pickup_at >= today() - INTERVAL 7 DAY
GROUP BY borough, zone
ORDER BY trips DESC
```

4. Name it **`CH Zone Dict Lookup`** and click **Save**.

> This dataset reads from `default.trips_raw` (raw table) rather than `analytics.fact_trips` to show the dictionary working at the source layer without any pre-processing.

---

## Part 2 — Create Charts

Create charts via **Charts → + Chart**, select the dataset, choose the chart type, configure the fields, then **Save** with the exact name listed.

---

### Dashboard 1 — CH Operations Command Center

#### Chart: CH Total Trips Today

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Big Number |
| Metric | COUNT(`trip_id`) |
| Time filter | `pickup_at` = `today : now` |
| Subheader | `trips today` |

Save as **`CH Total Trips Today`**.

#### Chart: CH Revenue Today

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Big Number |
| Metric | SUM(`fare_amount_usd`) |
| Time filter | `pickup_at` = `today : now` |
| Subheader | `revenue today` |

Save as **`CH Revenue Today`**.

#### Chart: CH Trip Volume by Hour (24h)

| Setting | Value |
|---------|-------|
| Dataset | `agg_hourly_zone_trips` |
| Chart type | Line Chart (ECharts) |
| X-axis | `hour_bucket` |
| Metric | SUM(`trips`) |
| Time grain | 1 hour |

Save as **`CH Trip Volume by Hour (24h)`**.

#### Chart: CH Revenue by Zone (Top 10)

| Setting | Value |
|---------|-------|
| Dataset | `agg_hourly_zone_trips` |
| Chart type | Bar Chart |
| Dimensions | `zone_id` |
| Metric | SUM(`revenue`) |
| Row limit | 10 |
| Sort bars | enabled |

Save as **`CH Revenue by Zone (Top 10)`**.

---

### Dashboard 2 — CH Executive Weekly Report

#### Chart: CH Daily Revenue (7 days)

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Line Chart (ECharts) |
| X-axis | `pickup_at` |
| Metric | SUM(`fare_amount_usd`) |
| Time grain | 1 hour |

Save as **`CH Daily Revenue (7 days)`**.

#### Chart: CH Top Zones by Revenue

| Setting | Value |
|---------|-------|
| Dataset | `agg_hourly_zone_trips` |
| Chart type | Bar Chart |
| Dimensions | `zone_id` |
| Metric | SUM(`revenue`) |
| Row limit | 20 |
| Sort bars | enabled |

Save as **`CH Top Zones by Revenue`**.

#### Chart: CH Payment Distribution

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Pie Chart |
| Dimensions | `payment_type` |
| Metric | COUNT(`trip_id`) |

Save as **`CH Payment Distribution`**.

#### Chart: CH Avg Fare by Vendor

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Bar Chart |
| Dimensions | `vendor_name` |
| Metric | AVG(`fare_amount_usd`) |
| Row limit | 20 |
| Sort bars | enabled |

Save as **`CH Avg Fare by Vendor`**.

---

### Dashboard 3 — CH Driver Quality Analytics

#### Chart: CH Rating Distribution

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Bar Chart |
| Dimensions | `driver_rating` |
| Metric | COUNT(`trip_id`) |
| Row limit | 20 |
| Sort bars | enabled |

Save as **`CH Rating Distribution`**.

#### Chart: CH High-Rated Driver Revenue

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Big Number |
| Metric | SUM(`fare_amount_usd`) |
| Filter | `driver_rating >= 4.5` |
| Subheader | `revenue from 4.5+ rated drivers` |

Save as **`CH High-Rated Driver Revenue`**.

#### Chart: CH Avg Fare by Driver Rating

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Bar Chart |
| Dimensions | `driver_rating` |
| Metric | AVG(`fare_amount_usd`) |
| Row limit | 20 |
| Sort bars | enabled |

Save as **`CH Avg Fare by Driver Rating`**.

#### Chart: CH Top Drivers Leaderboard

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Table |
| Query mode | Raw records |
| Columns | `vendor_name`, `vehicle_type`, `driver_rating`, `fare_amount_usd` |
| Row limit | 1000 |

Save as **`CH Top Drivers Leaderboard`**.

---

### Dashboard 4 — CH Capabilities Showcase

#### Chart: CH Recent Trips (fact_trips)

| Setting | Value |
|---------|-------|
| Dataset | `fact_trips` |
| Chart type | Table |
| Query mode | Raw records |
| Columns | `trip_id`, `pickup_at`, `dropoff_at`, `fare_amount_usd`, `pickup_borough`, `vendor_name` |
| Row limit | 1000 |

Save as **`CH Recent Trips (fact_trips)`**.

#### Chart: CH Fare Percentiles (quantileTDigest)

| Setting | Value |
|---------|-------|
| Dataset | `CH Fare Percentiles (quantileTDigest)` |
| Chart type | Table |
| Query mode | Raw records |
| Columns | `vendor_name`, `p50_fare`, `p95_fare`, `p99_fare`, `trip_count` |

Save as **`CH Fare Percentiles (quantileTDigest)`**.

#### Chart: CH Approx vs Exact Unique Trips (uniqHLL12)

| Setting | Value |
|---------|-------|
| Dataset | `CH Approx Unique Trips (uniqHLL12)` |
| Chart type | Table |
| Query mode | Raw records |
| Columns | `day`, `exact_unique_trips`, `approx_unique_trips`, `pct_error` |

Save as **`CH Approx vs Exact Unique Trips (uniqHLL12)`**.

#### Chart: CH Sampling Accuracy Demo (SAMPLE 0.1)

| Setting | Value |
|---------|-------|
| Dataset | `CH Sampling Demo` |
| Chart type | Table |
| Query mode | Raw records |
| Columns | `method`, `trip_count`, `avg_fare` |

Save as **`CH Sampling Accuracy Demo (SAMPLE 0.1)`**.

#### Chart: CH Zone Lookup via Dictionary (dictGet)

| Setting | Value |
|---------|-------|
| Dataset | `CH Zone Dict Lookup` |
| Chart type | Table |
| Query mode | Raw records |
| Columns | `borough`, `zone`, `trips`, `avg_fare` |

Save as **`CH Zone Lookup via Dictionary (dictGet)`**.

#### Chart: CH Weekly Revenue Trend (window functions)

| Setting | Value |
|---------|-------|
| Dataset | `CH Cohort Retention` |
| Chart type | Table |
| Query mode | Raw records |
| Columns | `week`, `pickup_borough`, `trips`, `revenue`, `rolling_4wk_avg_revenue` |

Save as **`CH Weekly Revenue Trend (window functions)`**.

---

## Part 3 — Assemble Dashboards

For each dashboard:

1. Go to **Dashboards → + Dashboard**.
2. Enter the title.
3. Click **Save** then **Edit Dashboard**.
4. From the right panel, drag each chart into the canvas.
5. Click **Save** when done.

### CH — Operations Command Center

**Title:** `CH — Operations Command Center`

| Row | Charts |
|-----|--------|
| Row 1 | CH Total Trips Today · CH Revenue Today |
| Row 2 | CH Trip Volume by Hour (24h) · CH Revenue by Zone (Top 10) |

### CH — Executive Weekly Report

**Title:** `CH — Executive Weekly Report`

| Row | Charts |
|-----|--------|
| Row 1 | CH Daily Revenue (7 days) · CH Top Zones by Revenue · CH Payment Distribution · CH Avg Fare by Vendor |

### CH — Driver Quality Analytics

**Title:** `CH — Driver Quality Analytics`

| Row | Charts |
|-----|--------|
| Row 1 | CH Rating Distribution · CH High-Rated Driver Revenue |
| Row 2 | CH Avg Fare by Driver Rating · CH Top Drivers Leaderboard |

### CH — Capabilities Showcase

**Title:** `CH — Capabilities Showcase`

| Row | Charts |
|-----|--------|
| Row 1 | CH Recent Trips (fact_trips) · CH Fare Percentiles (quantileTDigest) |
| Row 2 | CH Approx vs Exact Unique Trips (uniqHLL12) · CH Sampling Accuracy Demo (SAMPLE 0.1) |
| Row 3 | CH Zone Lookup via Dictionary (dictGet) · CH Weekly Revenue Trend (window functions) |

---

## Verification

After completing Part 3, open [http://localhost:8088](http://localhost:8088). Under **Dashboards**, you should see 7 total — 3 Snowflake dashboards (from Part 1 setup) and 4 prefixed `CH —`.

---

## Troubleshooting

**`fact_trips` or `agg_hourly_zone_trips` not found**
Run `dbt run` first (Step 7.3).

**`dictGet` returns empty strings**
The `analytics.taxi_zones_dict` dictionary has not been created. Run `scripts/04_create_dictionary.sql` (Step 7.4).

**Virtual dataset returns no rows**
The 30-day / 7-day window in some queries requires recent data. If your migration data is all historical, replace the time filter with a fixed date:
```sql
-- Replace: WHERE pickup_at >= today() - INTERVAL 30 DAY
-- With:    WHERE pickup_at >= '2023-01-01'
```

**403 error from the import script**
Your Superset session cookie has expired. Log out and back in, then re-run.

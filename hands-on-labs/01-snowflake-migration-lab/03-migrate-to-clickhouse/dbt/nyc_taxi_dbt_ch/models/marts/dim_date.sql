{{
  config(
    materialized = 'table',
    engine       = 'MergeTree()',
    order_by     = '(date_day)',
    schema       = 'analytics'
  )
}}

-- ════════════════════════════════════════════════════════════════════════════
-- dim_date — Date dimension: 2009-01-01 through 2029-12-31
--
-- SNOWFLAKE → CLICKHOUSE TRANSLATION SUMMARY:
--   dbt_utils.date_spine(...)  ->  numbers() table function
--   TO_CHAR(date_day, 'DY')         →  formatDateTime(date_day, '%a')
--   DAYOFWEEK(date_day)             →  toDayOfWeek(date_day)
--   MONTH(date_day)                 →  toMonth(date_day)
--   TO_CHAR(date_day, 'MON')        →  formatDateTime(date_day, '%b')
--   QUARTER(date_day)               →  toQuarter(date_day)
--   YEAR(date_day)                  →  toYear(date_day)
--   RIGHT(YEAR(...)::VARCHAR, 2)    →  substring(toString(toYear(...)), 3, 2)
--   CONCAT(...)                     →  concat(...)  (same name, lowercase)
--   TRUE / FALSE                    →  1 / 0  (ClickHouse uses UInt8 for booleans)
--   ::DATE cast on date_spine row   →  toDate() cast
--   VALUES (...) AS t (col, col)    →  VALUES ... without alias needed (inline CTE)
--
-- KEY DIFFERENCE — date spine generation:
--   Snowflake: dbt_utils.date_spine() macro (generates a SELECT with UNION ALL or
--              recursive CTE depending on adapter)
--   ClickHouse: numbers(N) generates integers 0..N-1. Adding toIntervalDay(number)
--               to a base Date is the idiomatic ClickHouse approach. Fast: ~7000 rows,
--               all computed in a single pass with no recursion.
-- ════════════════════════════════════════════════════════════════════════════

-- US Federal holidays (static reference — same data as Part 1, translated syntax)
-- Migration note: Snowflake used VALUES (...::DATE, '...') with PostgreSQL-style cast.
-- ClickHouse uses toDate('YYYY-MM-DD') for explicit date literals.
WITH us_holidays AS (
    SELECT toDate('2019-01-01') AS holiday_date, 'New Year''s Day'   AS holiday_name UNION ALL
    SELECT toDate('2019-01-21'), 'MLK Day'            UNION ALL
    SELECT toDate('2019-02-18'), 'Presidents'' Day'   UNION ALL
    SELECT toDate('2019-05-27'), 'Memorial Day'        UNION ALL
    SELECT toDate('2019-07-04'), 'Independence Day'    UNION ALL
    SELECT toDate('2019-09-02'), 'Labor Day'           UNION ALL
    SELECT toDate('2019-11-11'), 'Veterans Day'        UNION ALL
    SELECT toDate('2019-11-28'), 'Thanksgiving'        UNION ALL
    SELECT toDate('2019-12-25'), 'Christmas'           UNION ALL
    -- 2020
    SELECT toDate('2020-01-01'), 'New Year''s Day'     UNION ALL
    SELECT toDate('2020-01-20'), 'MLK Day'             UNION ALL
    SELECT toDate('2020-02-17'), 'Presidents'' Day'    UNION ALL
    SELECT toDate('2020-05-25'), 'Memorial Day'        UNION ALL
    SELECT toDate('2020-07-04'), 'Independence Day'    UNION ALL
    SELECT toDate('2020-09-07'), 'Labor Day'           UNION ALL
    SELECT toDate('2020-11-11'), 'Veterans Day'        UNION ALL
    SELECT toDate('2020-11-26'), 'Thanksgiving'        UNION ALL
    SELECT toDate('2020-12-25'), 'Christmas'           UNION ALL
    -- 2021
    SELECT toDate('2021-01-01'), 'New Year''s Day'     UNION ALL
    SELECT toDate('2021-01-18'), 'MLK Day'             UNION ALL
    SELECT toDate('2021-02-15'), 'Presidents'' Day'    UNION ALL
    SELECT toDate('2021-05-31'), 'Memorial Day'        UNION ALL
    SELECT toDate('2021-07-05'), 'Independence Day (observed)' UNION ALL
    SELECT toDate('2021-09-06'), 'Labor Day'           UNION ALL
    SELECT toDate('2021-11-11'), 'Veterans Day'        UNION ALL
    SELECT toDate('2021-11-25'), 'Thanksgiving'        UNION ALL
    SELECT toDate('2021-12-25'), 'Christmas'           UNION ALL
    -- 2022
    SELECT toDate('2022-01-01'), 'New Year''s Day'     UNION ALL
    SELECT toDate('2022-01-17'), 'MLK Day'             UNION ALL
    SELECT toDate('2022-02-21'), 'Presidents'' Day'    UNION ALL
    SELECT toDate('2022-05-30'), 'Memorial Day'        UNION ALL
    SELECT toDate('2022-07-04'), 'Independence Day'    UNION ALL
    SELECT toDate('2022-09-05'), 'Labor Day'           UNION ALL
    SELECT toDate('2022-11-11'), 'Veterans Day'        UNION ALL
    SELECT toDate('2022-11-24'), 'Thanksgiving'        UNION ALL
    SELECT toDate('2022-12-26'), 'Christmas (observed)' UNION ALL
    -- 2023
    SELECT toDate('2023-01-02'), 'New Year''s Day (observed)' UNION ALL
    SELECT toDate('2023-01-16'), 'MLK Day'             UNION ALL
    SELECT toDate('2023-02-20'), 'Presidents'' Day'    UNION ALL
    SELECT toDate('2023-05-29'), 'Memorial Day'        UNION ALL
    SELECT toDate('2023-07-04'), 'Independence Day'    UNION ALL
    SELECT toDate('2023-09-04'), 'Labor Day'           UNION ALL
    SELECT toDate('2023-11-10'), 'Veterans Day (observed)' UNION ALL
    SELECT toDate('2023-11-23'), 'Thanksgiving'        UNION ALL
    SELECT toDate('2023-12-25'), 'Christmas'
),

-- Migration note: Snowflake used dbt_utils.date_spine(datepart="day", ...)
-- which expands to a recursive CTE or UNION ALL depending on the adapter.
-- ClickHouse numbers(N) generates a column of UInt64 values [0, N-1] in one pass.
-- toDate('2009-01-01') + toIntervalDay(number) builds each date arithmetically.
-- dateDiff('day', start, end) computes the span — ~7670 rows for 2009–2029.
date_spine AS (
    SELECT
        toDate('2009-01-01') + toIntervalDay(number) AS date_day
    FROM numbers(
        toUInt32(dateDiff('day', toDate('2009-01-01'), toDate('2029-12-31'))) + 1
        -- +1 because dateDiff() excludes the end date; we want the spine to include 2029-12-31
    )
),

enriched AS (
    SELECT
        date_day,

        -- Snowflake: TO_CHAR(date_day, 'DY')  e.g. 'Mon', 'Tue'
        -- ClickHouse: formatDateTime with strftime-style format string
        formatDateTime(date_day, '%a')              AS day_of_week,

        -- Snowflake: DAYOFWEEK(date_day)  (0=Sunday in Snowflake)
        -- ClickHouse: toDayOfWeek(date_day)  (1=Monday by default, ISO 8601)
        -- Note: ClickHouse toDayOfWeek returns 1(Mon)–7(Sun). Snowflake returns 0(Sun)–6(Sat).
        -- Use toDayOfWeek(date_day, 0) for Sunday=0 mode to match Snowflake exactly.
        toDayOfWeek(date_day, 0)                    AS day_of_week_num,

        -- Snowflake: MONTH(date_day)
        toMonth(date_day)                           AS month_num,

        -- Snowflake: TO_CHAR(date_day, 'MON')  e.g. 'Jan', 'Feb'
        formatDateTime(date_day, '%b')              AS month_name,

        -- Snowflake: QUARTER(date_day)
        toQuarter(date_day)                         AS quarter_num,

        -- Snowflake: YEAR(date_day)
        toYear(date_day)                            AS year_num,

        -- Snowflake: CONCAT('FY', RIGHT(YEAR(date_day)::VARCHAR, 2), 'Q', QUARTER(date_day))
        -- ClickHouse: concat() + toString() + substring() for right-2-chars of year
        concat(
            'FY',
            substring(toString(toYear(date_day)), 3, 2),
            'Q',
            toString(toQuarter(date_day))
        )                                           AS fiscal_quarter,

        -- Snowflake: CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END
        -- ClickHouse: 1/0 UInt8 (no native BOOLEAN type; ClickHouse uses UInt8)
        -- toDayOfWeek(date_day, 0): 0=Sunday, 6=Saturday — matching Snowflake semantics
        if(toDayOfWeek(date_day, 0) IN (0, 6), 1, 0)  AS is_weekend,

        -- Snowflake: CASE WHEN h.holiday_date IS NOT NULL THEN TRUE ELSE FALSE END
        if(h.holiday_date IS NOT NULL, 1, 0)        AS is_holiday,

        h.holiday_name

    FROM date_spine
    LEFT JOIN us_holidays h ON date_spine.date_day = h.holiday_date
)

SELECT * FROM enriched

{{
  config(
    materialized = 'table',
    schema       = 'ANALYTICS'
  )
}}

-- Generate a date spine from 2009-01-01 to 2029-12-31
-- Covers full NYC Taxi dataset history plus forward planning
WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2009-01-01' as date)",
        end_date   = "cast('2029-12-31' as date)"
    ) }}
),

-- US Federal holidays (static reference, extended annually)
us_holidays AS (
    SELECT holiday_date, holiday_name FROM (VALUES
        -- 2019
        ('2019-01-01'::DATE, 'New Year''s Day'),
        ('2019-01-21'::DATE, 'MLK Day'),
        ('2019-02-18'::DATE, 'Presidents'' Day'),
        ('2019-05-27'::DATE, 'Memorial Day'),
        ('2019-07-04'::DATE, 'Independence Day'),
        ('2019-09-02'::DATE, 'Labor Day'),
        ('2019-11-11'::DATE, 'Veterans Day'),
        ('2019-11-28'::DATE, 'Thanksgiving'),
        ('2019-12-25'::DATE, 'Christmas'),
        -- 2020
        ('2020-01-01'::DATE, 'New Year''s Day'),
        ('2020-01-20'::DATE, 'MLK Day'),
        ('2020-02-17'::DATE, 'Presidents'' Day'),
        ('2020-05-25'::DATE, 'Memorial Day'),
        ('2020-07-04'::DATE, 'Independence Day'),
        ('2020-09-07'::DATE, 'Labor Day'),
        ('2020-11-11'::DATE, 'Veterans Day'),
        ('2020-11-26'::DATE, 'Thanksgiving'),
        ('2020-12-25'::DATE, 'Christmas'),
        -- 2021
        ('2021-01-01'::DATE, 'New Year''s Day'),
        ('2021-01-18'::DATE, 'MLK Day'),
        ('2021-02-15'::DATE, 'Presidents'' Day'),
        ('2021-05-31'::DATE, 'Memorial Day'),
        ('2021-07-05'::DATE, 'Independence Day (observed)'),
        ('2021-09-06'::DATE, 'Labor Day'),
        ('2021-11-11'::DATE, 'Veterans Day'),
        ('2021-11-25'::DATE, 'Thanksgiving'),
        ('2021-12-25'::DATE, 'Christmas'),
        -- 2022
        ('2022-01-17'::DATE, 'MLK Day'),
        ('2022-02-21'::DATE, 'Presidents'' Day'),
        ('2022-05-30'::DATE, 'Memorial Day'),
        ('2022-07-04'::DATE, 'Independence Day'),
        ('2022-09-05'::DATE, 'Labor Day'),
        ('2022-11-11'::DATE, 'Veterans Day'),
        ('2022-11-24'::DATE, 'Thanksgiving'),
        ('2022-12-26'::DATE, 'Christmas (observed)'),
        -- 2023
        ('2023-01-02'::DATE, 'New Year''s Day (observed)'),
        ('2023-01-16'::DATE, 'MLK Day'),
        ('2023-02-20'::DATE, 'Presidents'' Day'),
        ('2023-05-29'::DATE, 'Memorial Day'),
        ('2023-07-04'::DATE, 'Independence Day'),
        ('2023-09-04'::DATE, 'Labor Day'),
        ('2023-11-10'::DATE, 'Veterans Day (observed)'),
        ('2023-11-23'::DATE, 'Thanksgiving'),
        ('2023-12-25'::DATE, 'Christmas')
    ) t (holiday_date, holiday_name)
),

enriched AS (
    SELECT
        date_day::DATE                          AS date_day,
        TO_CHAR(date_day, 'DY')                AS day_of_week,
        DAYOFWEEK(date_day)                    AS day_of_week_num,
        MONTH(date_day)                        AS month_num,
        TO_CHAR(date_day, 'MON')               AS month_name,
        QUARTER(date_day)                      AS quarter_num,
        YEAR(date_day)                         AS year_num,
        CONCAT('FY', RIGHT(YEAR(date_day)::VARCHAR, 2), 'Q', QUARTER(date_day)) AS fiscal_quarter,
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END        AS is_weekend,
        CASE WHEN h.holiday_date IS NOT NULL THEN TRUE ELSE FALSE END           AS is_holiday,
        h.holiday_name
    FROM date_spine
    LEFT JOIN us_holidays h ON date_spine.date_day::DATE = h.holiday_date
)

SELECT * FROM enriched

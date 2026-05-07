-- ============================================================
-- Script 01: Create all tables in NYC_TAXI_DB
-- Run after: terraform apply
-- Run before: 02_seed_data.sql
-- ============================================================

USE WAREHOUSE TRANSFORM_WH;
USE DATABASE NYC_TAXI_DB;

-- ============================================================
-- RAW Layer
-- ============================================================

CREATE TABLE IF NOT EXISTS RAW.TRIPS_RAW (
    TRIP_ID           VARCHAR(36)    NOT NULL,  -- UUID generated on ingest
    VENDOR_ID         INTEGER,
    PICKUP_DATETIME   TIMESTAMP_NTZ  NOT NULL,
    DROPOFF_DATETIME  TIMESTAMP_NTZ  NOT NULL,
    PASSENGER_COUNT   INTEGER,
    TRIP_DISTANCE     FLOAT,
    RATECODE_ID       INTEGER,
    STORE_FWD_FLAG    VARCHAR(1),
    PU_LOCATION_ID    INTEGER,
    DO_LOCATION_ID    INTEGER,
    PAYMENT_TYPE      INTEGER,
    FARE_AMOUNT       FLOAT,
    EXTRA             FLOAT,
    MTA_TAX           FLOAT,
    TIP_AMOUNT        FLOAT,
    TOLLS_AMOUNT      FLOAT,
    TOTAL_AMOUNT      FLOAT,
    TRIP_METADATA     VARIANT,                  -- JSON: driver rating, app version, surge info
    INGESTED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE_TRUNC('month', PICKUP_DATETIME))
COMMENT = 'Raw NYC Taxi trip records — immutable source of truth';

-- ============================================================
-- ANALYTICS Layer — Dimension tables (small, fully loaded)
-- ============================================================
USE SCHEMA ANALYTICS;

CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_TAXI_ZONES (
    LOCATION_ID     INTEGER       NOT NULL PRIMARY KEY,
    BOROUGH         VARCHAR(50),
    ZONE            VARCHAR(100),
    SERVICE_ZONE    VARCHAR(50)
)
COMMENT = 'NYC TLC taxi zone lookup — 265 zones';

CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_DATE (
    DATE_DAY        DATE          NOT NULL PRIMARY KEY,
    DAY_OF_WEEK     VARCHAR(10),
    DAY_OF_WEEK_NUM INTEGER,
    MONTH_NUM       INTEGER,
    MONTH_NAME      VARCHAR(10),
    QUARTER_NUM     INTEGER,
    YEAR_NUM        INTEGER,
    FISCAL_QUARTER  VARCHAR(6),    -- e.g. FY27Q1
    IS_WEEKEND      BOOLEAN,
    IS_HOLIDAY      BOOLEAN,
    HOLIDAY_NAME    VARCHAR(100)
)
COMMENT = 'Date spine with fiscal periods and US federal holidays';

CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_PAYMENT_TYPE (
    PAYMENT_TYPE_ID   INTEGER      NOT NULL PRIMARY KEY,
    PAYMENT_CODE      VARCHAR(20),
    PAYMENT_DESC      VARCHAR(100)
)
COMMENT = 'Payment method lookup — 6 types';

CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_VENDOR (
    VENDOR_ID     INTEGER        NOT NULL PRIMARY KEY,
    VENDOR_CODE   VARCHAR(10),
    VENDOR_NAME   VARCHAR(100)
)
COMMENT = 'Taxi vendor / app provider — 3 vendors';

CREATE TABLE IF NOT EXISTS ANALYTICS.AGG_HOURLY_ZONE_TRIPS (
    HOUR_BUCKET     TIMESTAMP_NTZ NOT NULL,
    ZONE_ID         INTEGER       NOT NULL,
    TRIPS           INTEGER,
    REVENUE         FLOAT,
    AVG_DISTANCE    FLOAT,
    UPDATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (HOUR_BUCKET, ZONE_ID)
)
COMMENT = 'Pre-aggregated hourly zone trip metrics — refreshed every hour by dbt';

-- ============================================================
-- Seed static dimension data (TRUNCATE + INSERT for idempotency)
-- ============================================================

TRUNCATE TABLE ANALYTICS.DIM_PAYMENT_TYPE;
INSERT INTO ANALYTICS.DIM_PAYMENT_TYPE (PAYMENT_TYPE_ID, PAYMENT_CODE, PAYMENT_DESC) VALUES
    (1, 'CREDIT',  'Credit Card'),
    (2, 'CASH',    'Cash'),
    (3, 'NO_CHG',  'No Charge'),
    (4, 'DISPUTE', 'Dispute'),
    (5, 'UNKNOWN', 'Unknown'),
    (6, 'VOIDED',  'Voided Trip');

TRUNCATE TABLE ANALYTICS.DIM_VENDOR;
INSERT INTO ANALYTICS.DIM_VENDOR (VENDOR_ID, VENDOR_CODE, VENDOR_NAME) VALUES
    (1, 'CMT',  'Creative Mobile Technologies'),
    (2, 'VTS',  'VeriFone Inc.'),
    (3, 'DDS',  'Digital Dispatch Systems');

-- Verify
SELECT 'TRIPS_RAW created' AS status, COUNT(*) AS row_count FROM RAW.TRIPS_RAW
UNION ALL
SELECT 'DIM_PAYMENT_TYPE seeded', COUNT(*) FROM ANALYTICS.DIM_PAYMENT_TYPE
UNION ALL
SELECT 'DIM_VENDOR seeded', COUNT(*) FROM ANALYTICS.DIM_VENDOR;

# dbt in This Project: A Practical Guide

This document explains how dbt (data build tool) is used in the NYC Taxi Snowflake Migration Lab — what it does, why each piece exists, and how to reason about it.

---

## What dbt Does (and Doesn't Do)

dbt **transforms** data that is already in your database. It does not load data from outside, move files, or manage infrastructure. Its job is to take raw tables and turn them into clean, tested, analytics-ready tables — by running SQL you write.

Think of it as a build system for SQL. Each `.sql` file in `models/` is a model that becomes a table or view in Snowflake. dbt handles the `CREATE OR REPLACE` boilerplate, resolves dependencies between models, and runs your tests.

---

## Project Layout

```
dbt/nyc_taxi_dbt/
├── dbt_project.yml          # Project config: name, folder layout, materialization defaults
├── profiles.yml.example     # Connection config template (copy to ~/.dbt/profiles.yml)
├── packages.yml             # Third-party dbt packages
│
├── macros/
│   ├── generate_schema_name.sql   # Overrides dbt's default schema naming logic
│   └── generate_surrogate_key.sql # Wrapper for consistent surrogate key generation
│
└── models/
    ├── sources.yml          # Declares RAW.TRIPS_RAW as an external source
    │
    ├── staging/             # Layer 1: clean and rename raw columns
    │   ├── schema.yml       # Column-level tests for staging models
    │   ├── stg_trips.sql
    │   └── stg_taxi_zones.sql
    │
    ├── intermediate/        # Layer 2: joins and enrichment (no physical table)
    │   └── int_trips_enriched.sql
    │
    └── analytics/           # Layer 3: final tables consumed by dashboards
        ├── schema.yml
        ├── fact_trips.sql
        ├── agg_hourly_zone_trips.sql
        ├── dim_date.sql
        ├── dim_payment_type.sql
        ├── dim_taxi_zones.sql
        └── dim_vendor.sql
```

---

## The Three Layers (Medallion Architecture)

### Layer 1 — Staging (`models/staging/`)

**Purpose:** Take raw data exactly as it arrived and make it usable.

These models land in the `STAGING` schema as **views** (no storage cost — they run at query time). Each staging model does one job:

| Model | Source | What it does |
|-------|--------|--------------|
| `stg_trips` | `RAW.TRIPS_RAW` | Renames columns to snake_case, adds `duration_minutes`, flattens the VARIANT `TRIP_METADATA` column into typed columns |
| `stg_taxi_zones` | `ANALYTICS.DIM_TAXI_ZONES` | Light cleanup, adds `COALESCE` guards, provides a dbt lineage node |

The most important work here is flattening the `TRIP_METADATA` VARIANT column. Snowflake's colon-path syntax extracts nested JSON fields:

```sql
-- Snowflake: colon-path notation
TRIP_METADATA:driver.rating::FLOAT     AS driver_rating,
TRIP_METADATA:app.surge_multiplier::FLOAT AS surge_multiplier
```

This is one of the migration challenges — ClickHouse uses `JSONExtractFloat(TRIP_METADATA, 'driver', 'rating')` instead.

### Layer 2 — Intermediate (`models/intermediate/`)

**Purpose:** Perform all the joins in one place so they don't have to be repeated.

`int_trips_enriched` joins `stg_trips` to every dimension (zones, payment types, vendors, dates) and produces a wide, fully-denormalized row per trip. It is declared **ephemeral**, which means dbt inlines its SQL into whatever model references it — no physical table or view is created in Snowflake.

```sql
-- dbt_project.yml
intermediate:
  +materialized: ephemeral   # compiled inline, no CREATE TABLE
```

Use ephemeral when the intermediate result is only needed by one downstream model and you don't want to pay for the storage or query compile overhead.

### Layer 3 — Analytics (`models/analytics/`)

**Purpose:** Final, dashboard-ready tables.

These land in the `ANALYTICS` schema. There are two kinds:

**Static dimension tables** — small, fully reloaded on each `dbt run`:

| Model | Rows | Notes |
|-------|------|-------|
| `dim_date` | ~7,670 | Date spine 2009–2029, with fiscal quarters and US federal holidays |
| `dim_payment_type` | 6 | Passthrough from seed data |
| `dim_vendor` | 3 | Passthrough from seed data |
| `dim_taxi_zones` | 265 | Passthrough via `stg_taxi_zones` |

**Incremental fact/aggregate tables** — large, updated with MERGE on each run:

| Model | Rows | Notes |
|-------|------|-------|
| `fact_trips` | 50M | One row per trip, fully denormalized |
| `agg_hourly_zone_trips` | ~9M | Pre-aggregated hourly counts per zone |

---

## Materializations

A **materialization** controls what dbt creates in Snowflake for a given model.

| Materialization | What Snowflake object | When to use |
|----------------|----------------------|-------------|
| `view` | `CREATE VIEW` | Cheap; always reflects latest data; used for staging |
| `table` | `CREATE TABLE AS SELECT` | Full rebuild every run; used for small dimensions |
| `incremental` | `MERGE INTO` existing table | Large tables; only processes new rows |
| `ephemeral` | (no object — inlined as CTE) | Intermediate logic shared by one downstream model |

The two incremental models demonstrate different incremental strategies:

**`fact_trips`** — processes new trips since the last run:
```sql
{% if is_incremental() %}
  WHERE pickup_at > (SELECT MAX(pickup_at) FROM {{ this }})
{% endif %}
```

**`agg_hourly_zone_trips`** — re-aggregates a rolling 2-hour window to catch late-arriving data:
```sql
{% if is_incremental() %}
  WHERE pickup_at >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
{% endif %}
```

On the very first run (empty table), `is_incremental()` returns `false` and the full dataset is processed. On subsequent runs, only new data is processed. If the schema changes and you need to rebuild from scratch, run:

```bash
dbt run --full-refresh
```

---

## The MERGE Strategy (Key Migration Challenge)

When `incremental_strategy = 'merge'`, dbt generates a Snowflake `MERGE INTO` statement:

```sql
MERGE INTO ANALYTICS.FACT_TRIPS AS target
USING (SELECT ...) AS source
ON target.trip_id = source.trip_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

This is one of the most important migration challenges documented in the lab. **ClickHouse has no `MERGE` statement.** The equivalent in ClickHouse is to use a `ReplacingMergeTree` table engine and add `FINAL` to queries, or to use a `CollapsingMergeTree` for explicit insert/delete semantics.

---

## Schema Naming: The `generate_schema_name` Macro

dbt's default behavior concatenates the **target schema** from `profiles.yml` with the **custom schema** in `dbt_project.yml`:

```
target schema = STAGING  +  custom schema = ANALYTICS  →  STAGING_ANALYTICS  (wrong)
```

This project overrides that behavior with a custom macro in `macros/generate_schema_name.sql`:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema | upper }}      -- no custom schema → use target schema
  {%- else -%}
    {{ custom_schema_name | upper }}  -- custom schema → use it directly
  {%- endif -%}
{%- endmacro %}
```

Result: models with `+schema: ANALYTICS` land in `ANALYTICS`, not `STAGING_ANALYTICS`.

This macro is required any time you have multiple schemas in one dbt project and don't want the target schema name prepended.

---

## Connection and Credentials (`profiles.yml`)

dbt connects to Snowflake using a **profile** defined in `~/.dbt/profiles.yml` (never checked into git). The profile name in `dbt_project.yml` must match:

```yaml
# dbt_project.yml
profile: 'nyc_taxi'

# ~/.dbt/profiles.yml
nyc_taxi:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ORG') }}-{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      role: DBT_ROLE
      database: NYC_TAXI_DB
      warehouse: TRANSFORM_WH
      schema: STAGING       # ← this is the "target schema" / default schema
      threads: 4
```

Key points:
- `schema: STAGING` is the **default** schema. Models without a `+schema:` override land here.
- `role: DBT_ROLE` is a least-privilege role created by Terraform with only the permissions dbt needs.
- `threads: 4` controls how many models dbt builds in parallel.
- Credentials come from environment variables, loaded from `.env` before running setup.

---

## Testing

dbt tests come in two forms:

### Schema tests (declared in `schema.yml`)

```yaml
- name: trip_id
  tests:
    - not_null
    - unique
- name: total_amount_usd
  tests:
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: 0
        max_value: 1000
```

`not_null` and `unique` are built-in. `dbt_expectations` tests come from the `calogica/dbt_expectations` package declared in `packages.yml`.

### Custom SQL tests (`tests/`)

```sql
-- tests/assert_revenue_positive.sql
-- A passing test returns 0 rows
SELECT trip_id, total_amount_usd
FROM {{ ref('fact_trips') }}
WHERE total_amount_usd < 0
```

Custom tests are just SQL queries. dbt runs them and **fails if any rows are returned**.

Run all tests with:
```bash
dbt test
```

---

## Third-Party Packages (`packages.yml`)

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<1.0.0"]
```

Install them before first use:
```bash
dbt deps
```

`dbt_utils` provides the `date_spine` generator used in `dim_date.sql`. `dbt_expectations` provides range/distribution tests that go beyond the built-in `not_null`/`unique`.

---

## Dependency Graph

dbt builds models in the correct order automatically by following `{{ ref() }}` calls:

```
RAW.TRIPS_RAW (source — not managed by dbt)
    └── stg_trips (view)
            └── int_trips_enriched (ephemeral)
                    ├── fact_trips (incremental table)
                    └── agg_hourly_zone_trips (incremental table)

ANALYTICS.DIM_TAXI_ZONES (seeded by SQL script)
    └── stg_taxi_zones (view)
            ├── int_trips_enriched
            └── dim_taxi_zones (table)

dbt_utils.date_spine
    └── dim_date (table)
```

`{{ ref('stg_trips') }}` is how one model declares a dependency on another. `{{ source('raw', 'TRIPS_RAW') }}` declares a dependency on an external table (defined in `sources.yml`).

---

## Common Commands

| Command | What it does |
|---------|-------------|
| `dbt deps` | Install packages from `packages.yml` |
| `dbt run` | Build all models (incremental where possible) |
| `dbt run --full-refresh` | Rebuild all incremental models from scratch |
| `dbt run -s fact_trips` | Build only `fact_trips` and its dependencies |
| `dbt test` | Run all schema and custom tests |
| `dbt build` | `dbt run` + `dbt test` together |
| `dbt compile` | Generate SQL without executing (useful for debugging) |
| `dbt docs generate && dbt docs serve` | Build and browse the lineage graph in a browser |

In this project, `dbt run --full-refresh` is triggered automatically by `setup.sh` if `fact_trips` is empty (first run or after a tear-down).

---

## How dbt Fits Into the Full Setup

```
terraform apply          → creates warehouses, database, schemas, roles
scripts/01_create_tables.sql → creates raw tables, seeds dimension data
scripts/02_seed_data.sql → loads 50M synthetic trip rows
dbt deps && dbt build    → transforms raw data into analytics-ready tables
scripts/03_create_streams_tasks.sql → creates CDC stream and scheduled task
```

dbt sits in the middle of the pipeline. It cannot run until the raw tables exist and have data. The `setup.sh` script handles this ordering.

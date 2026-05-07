# NYC Taxi Migration Lab — Part 2: Plan & Design

This module guides you through the planning and architectural design decisions that determine whether your ClickHouse migration succeeds. You will profile the live Snowflake workload from Part 1, reason through ClickHouse-specific engine and schema choices, and produce a `migration-plan.md` that Part 3 then executes.

**No new infrastructure is provisioned here.** You use the same Snowflake environment from Part 1.

---

## Why This Module Exists

The most common reason ClickHouse migrations underperform is not a tuning problem — it is an architecture problem. Partners move data first and think about design later. By the time they realize that the wrong MergeTree engine silently produces incorrect results, or that a sort key derived from the source schema ignores actual query patterns, the migration is already "done."

This module embodies **PRISM Phases P (Profile) and R (Re-architect)**:

- **Profile**: Understand *what you have* — inventory every object, identify every SQL construct that needs translation, measure the actual query workload.
- **Re-architect**: Make *explicit decisions* about engines, sort keys, and schema mapping before writing a single line of ClickHouse SQL.

The `migration-plan.md` you produce is the bridge. Part 3's Decision Alignment table maps every implementation choice in that lab directly back to these decisions — so you can see your reasoning manifest as working code.

---

## Module Output

One file: **`migration-plan.md`** — your documented decisions, ready for Part 3.

You fill this in section by section as you complete the worksheets. When all four checkboxes are checked, you are ready to proceed.

---

## Flow

```
1. Run profiling script → profile_report.md     (15 min)
2. Work through 4 worksheets                    (60-90 min)
3. Fill in migration-plan.md                    (15 min)
4. Proceed to Part 3
```

Total estimated time: **105–140 minutes**

---

## Prerequisites

- Part 1 environment running (Snowflake credentials in `01-setup-snowflake/.env`)
- `snowsql` installed and on your PATH
- `SNOWFLAKE_ORG`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` in your environment

Source Part 1's `.env` before running anything:

```bash
source ../01-setup-snowflake/.env
```

---

## Step 1 — Profile the Snowflake Environment

Run the profiling script against your live Part 1 Snowflake instance:

```bash
source ../01-setup-snowflake/.env
./scripts/01_profile_snowflake.sh
```

This generates `profile_report.md` with four sections:
1. **Object Inventory** — every table, view, stream, and task with row counts and complexity grades
2. **Query Workload** — top 10 queries by total elapsed time from the last 7 days
3. **Table Statistics** — row counts, date ranges, null rates, VARIANT column usage
4. **Schema Compatibility Gaps** — auto-detected constructs that require ClickHouse translation

If `ACCOUNT_USAGE` is unavailable (requires 1–3 hr propagation delay or ACCOUNTADMIN role), the script falls back to `INFORMATION_SCHEMA` and notes what it couldn't measure. You can also run `scripts/02_query_history.sql` manually in the Snowflake UI.

---

## Step 2 — Complete the Worksheets

Work through the four worksheets in order. Each one teaches a concept, then has fill-in exercises for the actual NYC Taxi workload. Answer keys are in collapsible sections at the bottom of each worksheet — try the exercises before looking.

| Worksheet | Concept | What you produce |
|-----------|---------|------------------|
| [`worksheets/01_mergetree_engine_selection.md`](worksheets/01_mergetree_engine_selection.md) | MergeTree engine family | Engine choice + reasoning per table |
| [`worksheets/02_sort_key_design.md`](worksheets/02_sort_key_design.md) | ORDER BY from query workload | Sort key per table |
| [`worksheets/03_schema_translation.md`](worksheets/03_schema_translation.md) | Type mapping + function translation | Type for each column; CH equivalent for each SF expression |
| [`worksheets/04_migration_wave_plan.md`](worksheets/04_migration_wave_plan.md) | Dependency ordering | Wave table: object → wave → dependencies |
| [`worksheets/05_dbt_model_design.md`](worksheets/05_dbt_model_design.md) | dbt model configuration | Materialization, engine, incremental strategy, and FINAL placement per model |

---

## Step 3 — Complete migration-plan.md

Open [`migration-plan.md`](migration-plan.md) and fill in each section using your worksheet answers. The document has nine sections and four completion checkboxes:

```markdown
- [ ] Engine selection: completed
- [ ] Sort key design: completed
- [ ] Schema translation: completed
- [ ] Migration wave plan: completed
```

When all four are checked, this module is complete.

---

## Reference Documents

These are reference-only — not exercises. Read them when you need background or want to check a specific detail during the worksheets.

| Document | What it covers |
|----------|---------------|
| [`docs/snowflake_vs_clickhouse.md`](docs/snowflake_vs_clickhouse.md) | Architecture comparison; all 6 SQL dialect gaps with side-by-side examples |
| [`docs/mergetree_guide.md`](docs/mergetree_guide.md) | MergeTree family deep dive: MergeTree, ReplacingMergeTree, AggregatingMergeTree, TTL |
| [`docs/dbt_clickhouse_guide.md`](docs/dbt_clickhouse_guide.md) | dbt-clickhouse patterns: materialization types, engine config, `delete_insert` mechanics, FINAL placement, `generate_schema_name` macro |

---

## Worked Example

[`examples/nyc_taxi_completed_plan.md`](examples/nyc_taxi_completed_plan.md) contains the fully worked-out answers for all four worksheets, applied to the NYC Taxi workload.

Use it to:
- Check your worksheet answers after completing each section
- Understand the reasoning behind choices that Part 3 implements
- Compare against Part 3's Decision Alignment table if you chose differently

---

## What Happens If You Skip This Module

Part 3's `setup.sh` checks for `migration-plan.md` and warns if it's missing or incomplete. It never blocks — partners who already know ClickHouse can proceed directly. But if you are new to ClickHouse, skipping this module means:

- You will execute the migration without knowing *why* `fact_trips` uses `ReplacingMergeTree` instead of `MergeTree` — and not recognizing when that choice would be wrong for a different workload
- You will see the ORDER BY on `fact_trips` and not know how it was derived from query patterns
- You will see dbt configs in Part 3 (`delete_insert`, `ReplacingMergeTree` engine, `FINAL` in `stg_trips`) without knowing how to derive them for a different workload
- The benchmark results will show speedups you cannot explain or reproduce for a customer

# NYC Taxi ClickHouse Migration Lab

A four-part, hands-on lab for ClickHouse Solutions Architects and partners learning to migrate a production-grade Snowflake workload to ClickHouse Cloud.

The dataset is a synthetic NYC taxi ride stream: 50 million rows, a full Medallion dbt pipeline, seven complex analytical queries, live data producers, and Superset dashboards — built to simulate the kind of workload you will encounter in real customer engagements.

---

## What This Lab Covers

| Part | Name | What you do |
|------|------|-------------|
| **[1 — Source Environment Setup](01-setup-snowflake/README.md)** | Source Environment Setup | Provision a Snowflake environment that mirrors a real customer deployment — including a dbt Medallion pipeline, live trip producer, and BI dashboards |
| **[2 — Plan & Design](02-plan-and-design/README.md)** | Plan & Design | Profile the Snowflake workload and make explicit ClickHouse architecture decisions: engine selection, sort key design, schema translation, deployment waves, and dbt model configuration |
| **[3 — Migrate to ClickHouse Cloud](03-migrate-to-clickhouse/README.md)** | Migrate to ClickHouse Cloud | Execute the migration — provision ClickHouse Cloud with Terraform, move 50M rows, rebuild the dbt pipeline with ClickHouse-native engines, recreate dashboards, and benchmark performance |
| **[4 — Evaluation](04-evaluation/README.md)** | Evaluation | Complete a 20-question MCQ + 4 open-question assessment to earn the **ClickHouse Migration Proficiency Badge** |

---

## What This Lab Does Not Cover

This lab is scoped to the core migration pattern. The following topics are intentionally out of scope:

- **Real-time streaming ingestion** — The lab uses a Docker-based trip producer for live writes post-cutover. It does not cover Kafka, Kinesis, or ClickPipes streaming sources.
- **Multi-node ClickHouse clusters** — All work is on a ClickHouse Cloud single-service deployment. Self-hosted distributed deployments (sharding, replication topology) are not covered.
- **Data governance and access control** — Role-based access, row-level security, and masking policies are not implemented.
- **Incremental schema evolution** — The lab uses a fixed schema throughout. Handling live schema changes during migration is not covered.
- **Non-Snowflake sources** — The migration pattern is Snowflake-specific. PostgreSQL, MySQL, BigQuery, and other sources have different SQL dialects and CDC approaches.
- **Production SLA and monitoring** — Observability, alerting, and SLA management in production ClickHouse Cloud are not covered.

---

## Learning Objectives

By completing all four parts, you will be able to:

1. **Profile a Snowflake workload** — inventory tables, identify query patterns, spot SQL dialect gaps, and size the migration effort
2. **Make correct engine and schema decisions** — choose the right MergeTree engine family for each table, design effective `ORDER BY` keys, and translate Snowflake types and SQL idioms to ClickHouse equivalents
3. **Execute a production-style migration** — move 50M rows with a resumable Python script, validate parity, and perform a scripted producer cutover
4. **Rebuild a dbt pipeline on ClickHouse** — configure `dbt-clickhouse` with `delete_insert` strategy, `ReplacingMergeTree` models, and Refreshable Materialized Views
5. **Quantify the business case** — run a seven-query benchmark and articulate 6–9x performance improvements to customers
6. **Explain your decisions** — pass the Part 4 assessment, demonstrating you can defend design choices and apply the migration pattern to new customer workloads

---

## How to Use This Lab

### Prerequisites

Before starting, install the required tools:

| Tool | Version | Purpose |
|------|---------|---------|
| [Terraform](https://developer.hashicorp.com/terraform/install) | ≥ 1.6 | Provision Snowflake and ClickHouse Cloud infrastructure |
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | ≥ 24 | Run the trip producer and Superset |
| [Python](https://www.python.org/downloads/) | 3.11–3.13 | Migration script, dbt, and utility scripts |
| [dbt Core](https://docs.getdbt.com/docs/core/installation-overview) | ≥ 1.8 | Snowflake and ClickHouse pipeline |
| [SnowSQL CLI](https://docs.snowflake.com/en/user-guide/snowsql-install-config) | ≥ 1.2 | Run SQL against Snowflake from the terminal |
| [Snowflake account](https://signup.snowflake.com/) | Trial or paid — **no credit card required** | Source environment |
| [ClickHouse Cloud account](https://clickhouse.cloud/signUp) | Trial or paid — **no credit card required** | Target environment |

> **Python version matters:** dbt-snowflake requires Python **3.11, 3.12, or 3.13**. Python 3.14+ breaks dbt's `mashumaro` dependency. If your system Python is 3.14+, install 3.13 separately (e.g. `brew install python@3.13`).
>
> Package installation is covered in each part's README using an isolated virtualenv — do not run a bare `pip install` globally.

### Recommended Path

Work through the parts in order. Each part builds on the previous one.

```
Part 1 (≈ 45 min)
  └─▶ Part 2 (≈ 90 min)
        └─▶ Part 3 (≈ 120 min)
              └─▶ Part 4 (≈ 60 min)
```

**[Part 1](01-setup-snowflake/README.md)** provisions the Snowflake environment and must be running throughout Parts 2 and 3 — the live producer writes new trip rows continuously and the dbt Snowflake pipeline runs against it.

**[Part 2](02-plan-and-design/README.md)** produces a `migration-plan.md` document. Part 3's `setup.sh` checks for this file. Skipping Part 2 means skipping the architectural reasoning that makes Part 3 meaningful.

**[Part 3](03-migrate-to-clickhouse/README.md)** provisions ClickHouse Cloud (Terraform), migrates data, and runs the benchmark. Tear down with `teardown.sh` when finished to avoid ongoing cloud costs.

**[Part 4](04-evaluation/README.md)** is self-administered and open-book. Submit your completed `assessment.md` to your ClickHouse Solutions Architect for grading.

### Start Here

```bash
# Clone the repository
git clone https://github.com/ClickHouse/partner-labs.git
cd partner-labs/01-snowflake-migration-lab

# Begin with Part 1
cd 01-setup-snowflake
cat README.md
```

Each part's `README.md` contains all setup instructions, step-by-step commands, verification queries, and troubleshooting guidance.

---

## Estimated Time and Cost

| Part | Wall-clock time | Snowflake credits | ClickHouse Cloud |
|------|----------------|-------------------|-----------------|
| [1 — Setup](01-setup-snowflake/README.md) | ~45 min | ~2–4 credits | — |
| [2 — Plan & Design](02-plan-and-design/README.md) | ~90 min | ~0.5 credits | — |
| [3 — Migrate](03-migrate-to-clickhouse/README.md) | ~120 min | ~3–5 credits | ~$2–4 (trial) |
| [4 — Evaluation](04-evaluation/README.md) | ~60 min | — | — |
| **Total** | **~5–6 hours** | **~6–10 credits** | **~$2–4** |

*Snowflake credit estimates assume a standard X-Small warehouse. ClickHouse Cloud cost assumes a Development tier service torn down within a few hours.*

---

## Repository Structure

```
01-snowflake-migration-lab/
├── 01-setup-snowflake/         Part 1: Snowflake source environment
│   ├── setup.sh                Provisions Snowflake (Terraform + dbt + producer + Superset)
│   ├── teardown.sh
│   ├── terraform/              Warehouses, roles, database, schemas
│   ├── dbt/nyc_taxi_dbt/       Medallion pipeline: RAW → STAGING → ANALYTICS
│   ├── producer/               Docker service writing live trip rows to Snowflake
│   ├── queries/                7 annotated benchmark queries with migration challenges
│   └── docs/                   Architecture diagrams, migration assessment template
│
├── 02-plan-and-design/         Part 2: Architecture planning
│   ├── migration-plan.md       Output document — partner fills this in
│   ├── worksheets/             5 guided teaching modules with exercises and answer keys
│   ├── docs/                   Reference: MergeTree guide, SQL dialect gaps, dbt-clickhouse
│   ├── scripts/                Snowflake profiling script → profile_report.md
│   └── examples/               Completed worked example (nyc_taxi_completed_plan.md)
│
├── 03-migrate-to-clickhouse/   Part 3: Migration execution
│   ├── setup.sh                Orchestrates all 8 steps
│   ├── teardown.sh
│   ├── terraform/              ClickHouse Cloud service + IP access list
│   ├── scripts/                Migration script, parity check, benchmark, cutover
│   ├── dbt/nyc_taxi_dbt_ch/    ClickHouse dbt pipeline (delete_insert, ReplacingMergeTree)
│   ├── producer/               ClickHouse trip producer (post-cutover)
│   ├── superset/               Docker Compose Superset with 4 ClickHouse dashboards
│   └── docs/                   Architecture diagrams, migration runbook
│
├── 04-evaluation/              Part 4: Badge assessment
│   ├── assessment.md           20 MCQ + 4 open questions (partner fills in)
│   └── docs/answer-key.md      SA-only answer key and grading rubric
│
└── common/
    └── scripts/                Shared utilities (diagram rendering)
```

---

## Getting Help

- Each part's README has a **Troubleshooting** section covering the most common issues: [Part 1](01-setup-snowflake/README.md) · [Part 2](02-plan-and-design/README.md) · [Part 3](03-migrate-to-clickhouse/README.md)
- Reach out to your ClickHouse Solutions Architect with questions or to schedule a walkthrough session

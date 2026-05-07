# ClickHouse Partner Labs

A collection of self-paced, hands-on labs for ClickHouse technical partners. Each lab is delivered as a self-contained directory: clone the repo, open the lab folder, and follow its `README.md`.

## Available Labs

| Lab | Description |
|---|---|
| [01-snowflake-migration-lab](01-snowflake-migration-lab/) | Migrate an analytics workload from Snowflake to ClickHouse. |

## Lab Structure

Every lab in this repo follows the same four-part structure:

1. **Part 1 — Source Environment Setup:** Stand up the "before" state with production-realistic features.
2. **Part 2 — Architectural Analysis:** Inspect the source, map concepts to ClickHouse, and write an ADR.
3. **Part 3 — Migration Execution:** Provision the target, run in parallel, validate, and cut over.
4. **Part 4 — Knowledge Validation:** Multiple-choice and open-ended assessment questions.

Each lab folder contains its own prerequisites, time estimates, and cleanup script. Start with the lab's root `README.md`.

## Getting Started

1. Clone this repo.
2. Pick a lab from the table above.
3. Open that lab's `README.md` and follow the prerequisites and Part 1 instructions.
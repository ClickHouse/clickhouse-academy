# Part 4 — Migration Proficiency Evaluation

## Overview

This is the capstone assessment for the **NYC Taxi ClickHouse Migration Lab**. By completing Parts 1–3 you have:

- Profiled a production-grade Snowflake workload (50M rows, 7 complex queries, dbt Medallion pipeline)
- Designed a ClickHouse schema with explicit engine, sort key, and dbt configuration decisions
- Executed a full migration, rebuilt the analytics pipeline, and benchmarked the result

This evaluation measures whether you can **explain and defend** those decisions — not just follow steps. Passing earns you the **ClickHouse Migration Proficiency Badge**.

---

## Prerequisites

Before starting, confirm you have completed all three parts:

- [ ] Part 1 — Snowflake setup running, all 7 queries executed
- [ ] Part 2 — All 5 worksheets complete, `migration-plan.md` filled in with all checkboxes checked
- [ ] Part 3 — ClickHouse Cloud service live, 50M rows migrated, benchmark results in `scripts/benchmark_results_*.csv`, dashboards working

You will need to attach your **`migration-plan.md`** (Part 2) and **`benchmark_results_*.csv`** (Part 3) when you submit. Questions draw on knowledge from across all three parts.

---

## How to Complete

1. Make a copy of [`assessment.md`](assessment.md) — name it `assessment_<your_name>.md`
2. Answer all **20 multiple-choice questions** — write your letter choice (A, B, C, or D) on the `**Your answer:**` line
3. Answer all **4 open questions** — write in the space provided; aim for 3–6 sentences per sub-question
4. Fill in the submission checklist at the bottom of the file
5. Email the completed file plus required attachments to your ClickHouse Solutions Architect

You may reference your completed `migration-plan.md`, benchmark results, and the lab READMEs while answering — this is open-book. The open questions in particular reward depth of reasoning, not recall.

**Estimated time: 45–60 minutes**

---

## Scoring

| Section | Questions | Points each | Total |
|---------|-----------|-------------|-------|
| Section A — MCQ | 20 | 4 | 80 |
| Section B — Open | 4 | 5 | 20 |
| **Total** | **24** | | **100** |

**Pass threshold: 80 / 100**

- MCQ: no partial credit — 4 pts or 0 pts per question
- Open questions: SA grades 0–5 using a rubric (see `docs/answer-key.md`)

---

## After Submission

Your SA will review the submission against the answer key and rubric and reply within **5 business days**.

| Score | Outcome |
|-------|---------|
| 80–100 | **Pass** — ClickHouse Migration Proficiency Badge issued |
| 70–79 | **Near-miss** — SA provides targeted feedback; one retry on open questions |
| < 70 | **Not yet** — SA identifies weak areas; retake full assessment after review |

The badge certifies that you can confidently guide customers through a Snowflake → ClickHouse migration: profiling the workload, making correct engine and schema decisions, executing the migration, and validating the result.

---

## For Solutions Architects

The answer key and open-question rubric are in [`docs/answer-key.md`](docs/answer-key.md). Do not share this file with partners.

When grading open questions, apply the rubric holistically — look for correct reasoning, not specific wording. A partner who arrives at the right answer through a different but valid path should receive full credit.

# pg_trickle — Architecture & Roadmap Index

> The implementation plan that used to live here has been archived.  
> See [archive/PLAN_HISTORICAL.md](archive/PLAN_HISTORICAL.md) for the v0.9.0 era content.

## Active Planning Documents

| Document | Purpose |
|---|---|
| [ROADMAP.md](../ROADMAP.md) | Current release milestones and feature backlog |
| [roadmap/](../roadmap/) | Per-version detailed roadmap files |
| [PLAN_0_105_0.md](PLAN_0_105_0.md) | v0.105.0 qualification contract and release evidence |
| [PLAN_0_105_1.md](PLAN_0_105_1.md) | v0.105.1 delegated Graph V1 authorization, runtime conformance, and recovery qualification |
| [PLAN_0_105_2.md](PLAN_0_105_2.md) | v0.105.2 package, upgrade, performance, and field validation |
| [pg-trickle assessment and pre-1.0 roadmap](pg-trickle-assessment-and-pre-1.0-roadmap.md) | September 2026 assessment that drives v0.99.0 through v0.105.2 |
| [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) | System architecture and design overview |
| [INDEX.md](INDEX.md) | Full index of all plans, assessments, and ADRs |
| [adrs/](adrs/) | Architecture Decision Records |

## Key Architecture Docs

| Document | Topic |
|---|---|
| [docs/DVM_OPERATORS.md](../docs/DVM_OPERATORS.md) | Differential view maintenance operators |
| [docs/DVM_REWRITE_RULES.md](../docs/DVM_REWRITE_RULES.md) | Rewrite rules for DVM |
| [docs/COST_MODEL.md](../docs/COST_MODEL.md) | Refresh cost model and AUTO-mode decision logic |
| [docs/GUC_CATALOG.md](../docs/GUC_CATALOG.md) | Generated GUC reference |
| [docs/LIMITATIONS.md](../docs/LIMITATIONS.md) | Known limitations and unsupported SQL patterns |
| [docs/COMPARISONS.md](../docs/COMPARISONS.md) | pg_trickle vs pg_ivm, Materialize, Feldera, DuckDB/DuckLake |
| [plans/PLAN_OVERALL_ASSESSMENT_14.md](PLAN_OVERALL_ASSESSMENT_14.md) | v0.74.0 deep-assessment findings (drives v0.72–v0.75 arc) |

## Historical Archive

Archived plans from past implementation cycles live in [archive/](archive/).

# plans/ — Document Guidelines

Rules for creating and maintaining planning documents in this folder.
Existing documents are **not** retroactively renamed — these conventions apply
to new documents and should be adopted when an existing document gets a
significant update.

See [INDEX.md](INDEX.md) for a full inventory of all documents with their
types and statuses.

---

## Document Types

Every document has **exactly one** type, expressed as a filename prefix.

| Prefix | What it is | When to use | Examples |
|--------|-----------|-------------|---------|
| `PLAN_` | Implementation plan with concrete phases, steps, and acceptance criteria. | You know *what* to build and need to describe *how*. | `PLAN_HYBRID_CDC.md`, `PLAN_PACKAGING.md` |
| `GAP_` | Gap analysis — identifies what is missing relative to a competitor, standard, SQL spec, or target state. | You need to compare current capabilities against a reference and catalogue deficits. | `GAP_ANALYSIS_EPSIO.md`, `GAP_SQL_PHASE_7.md` |
| `REPORT_` | Research, investigation, feasibility study, options analysis, comparison, or assessment. Reference material — not directly actionable. | You explored a topic and need to record findings for future reference. | `REPORT_PARALLELIZATION.md`, `REPORT_TRIGGERS_VS_REPLICATION.md` |
| `STATUS_` | Point-in-time progress snapshot or tracking dashboard for an ongoing area. | You need a current view of a topic's status. | `STATUS_PERFORMANCE.md` |

**Choosing between types:**

- If the document's primary value is "what's missing" → `GAP_`.
- If it explores options or compares approaches without committing to one → `REPORT_`.
- If it describes *what to build and how* → `PLAN_`.
- A GAP analysis often feeds into a PLAN. They can be separate documents or
  combined — use `GAP_` when the gap catalogue is the core artifact.
- If a document doesn't fit any type, it probably belongs in `docs/`
  (user-facing) or is a `REPORT_`.

---

## Filename Convention

```
<PREFIX>_<TOPIC>[_<QUALIFIER>].md
```

- **PREFIX** — One of `PLAN_`, `GAP_`, `REPORT_`, `STATUS_`.
- **TOPIC** — Two-to-four `UPPER_SNAKE_CASE` words describing the subject.
- **QUALIFIER** (optional) — Version, part number, or narrowing scope.
- Always `.md`.

Good:
```
PLAN_TRANSACTIONAL_IVM.md
REPORT_TRIGGERS_VS_REPLICATION.md
GAP_ANALYSIS_FELDERA.md
STATUS_PERFORMANCE.md
```

Avoid:
```
citus.md                    # no prefix, lowercase
PLAN performance part 8.md  # spaces, no underscores
PERF_RESULTS.md             # non-standard prefix
```

### Iterative / Multi-Part Documents

For evolving analyses that produce numbered iterations, include the sequence
in the qualifier:

```
GAP_SQL_PHASE_4.md
GAP_SQL_PHASE_5.md
PLAN_PERFORMANCE_PART_9.md
```

---

## Folder Structure

Documents are organized by **topic area**, not by document type.

```
plans/
├── README.md                 ← this file (guidelines)
├── INDEX.md                  ← full document inventory with statuses
├── PLAN.md                   ← master implementation plan (top-level only)
├── adrs/                     ← Architecture Decision Records (single collection file)
├── dbt/                      ← dbt adapter & macros
├── ecosystem/                ← Competitor analysis, integrations, compatibility
├── infra/                    ← CI/CD, packaging, deployment, Docker, costs
├── performance/              ← Benchmarks, optimization, profiling
├── sql/                      ← SQL features, syntax, operators, CDC
└── testing/                  ← Test strategy, suites, coverage
```

**Rules:**

1. A document lives in the folder matching its **primary topic**, regardless
   of document type. A `GAP_` about SQL features goes in `sql/`, not a
   separate `gaps/` folder.
2. `PLAN.md` (the master plan) stays at the `plans/` root. No other documents
   at the root unless they span all topic areas.
3. Create a new subfolder only when there are **3+ documents** that don't fit
   an existing folder. Discuss in PR before adding.

---

## Checklist for New Documents

- [ ] Filename matches `<PREFIX>_<TOPIC>.md` convention
- [ ] Placed in the correct topic subfolder
- [ ] Has a clear status field near the top
- [ ] Linked from related documents where relevant
- [ ] Added to [INDEX.md](INDEX.md)

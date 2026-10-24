# pg_trickle documentation brief

Write a concise English engineering wiki for contributors to this PostgreSQL
extension. Explain behavior, invariants, and how to make changes safely. Keep
existing documentation authoritative for user-facing SQL and configuration;
link to it instead of reproducing catalogs or installation instructions.

## Evidence and scope

- Start with `AGENTS.md`, `Cargo.toml`, `README.md`, `ESSENCE.md`,
  `docs/ARCHITECTURE.md`, and `justfile`. Follow current source and focused tests
  before asserting behavior. Resolve versions from manifests, not old plans.
- Use `src/`, `sql/`, `tests/`, `scripts/`, and `.github/workflows/` as primary
  evidence. Read relevant decisions in `plans/adrs/`; treat `ROADMAP.md`,
  `roadmap/`, and other plans as intent until implementation confirms them.
- Honor `.openwikiignore`. Stay within this repository. Do not read user home
  directories, credentials, local databases, external connectors, or private
  Promise-to-Proof state. Keep checked-in environment examples available.
- Research representative flows and failure paths. Avoid exhaustive symbol
  inventories, changelog summaries, benchmark data dumps, and one page per file.

## Coverage priorities

Organize a small set of linked pages around these topics; combine related topics
when that makes navigation easier:

1. A quickstart routing contributors to source owners, existing references, and
   the relevant build and test commands.
2. The stream table lifecycle: SQL API boundaries, catalog state, dependency
   DAGs, creation, alteration, refresh, and removal.
3. Differential view maintenance: query validation and rewrites, operator plans,
   row identity and multiplicity, delta generation, and supported SQL boundaries.
4. CDC and refresh correctness: trigger capture, optional WAL capture, change
   buffers, frontiers, transaction boundaries, delta application, cleanup,
   recovery, and downstream consumers.
5. Scheduling and operations: workers, concurrency and memory ownership,
   freshness, configuration, diagnostics, monitoring, backup, and upgrades.
6. Security and verification: privilege boundaries, SECURITY DEFINER behavior,
   error handling, unit/integration/E2E tiers, and regression coverage. Cover dbt
   integration in a focused page if needed, following `dbt-pgtrickle/AGENTS.md`.

## Non-negotiable facts to verify

- Correctness and durability take precedence over latency and throughput.
  Document differential refresh and its fallback conditions precisely.
- Default statement-level AFTER trigger capture writes changes in the source
  transaction. Verify visibility, rollback, frontier advancement, and buffer
  cleanup against current code and tests; do not imply that batching commits
  partial refresh results or releases locks early.
- Distinguish tested guarantees from intended behavior and untested assumptions.
  Cite bounded source evidence for material claims and name useful focused tests.
  Never report tests as passed unless they were actually run.
- Use diagrams only for meaningful flows or state transitions. Link neighboring
  wiki pages and the existing SQL, GUC, architecture, and limitations references.

## Maintenance

Use OpenWiki's source-backed Claims and resumable page lifecycle. Update existing
pages incrementally and preserve accurate content. Keep this brief user-owned.
Generation is documentation-only: do not edit implementation, existing reference
docs, or workflows; do not commit, push, publish, or schedule updates. Let OpenWiki
manage its own metadata, indexes, and setup blocks.

# Documentation Review Checklist

## Contents
- Per-file checklist
- Audience & prerequisites checklist
- Rendering checklist
- Severity definitions
- Terminology ground truth
- Common false positives

---

## Per-file checklist

For each Markdown file in `docs/`, verify:

### Accuracy
- [ ] All SQL function names match `src/api.rs` (`#[pg_extern]` annotations)
- [ ] All GUC names match `src/config.rs` — include the full dotted name e.g. `pg_trickle.enabled`
- [ ] All default values match `src/config.rs` GUC defaults
- [ ] Schema names are `pgtrickle` (catalog) and `pgtrickle_changes` (change buffers)
- [ ] Version numbers are consistent with `pg_trickle.control` and `Cargo.toml`
- [ ] Limitations match `src/dvm/parser/validation.rs` (volatility checks, unsupported clauses)
- [ ] CDC trigger description matches `src/cdc.rs` and `src/wal_decoder.rs`
- [ ] Refresh mode descriptions match `src/refresh.rs`

### Links
- [ ] All relative links resolve to existing files
- [ ] Anchor links (`#section`) resolve (section heading exists in target file)
- [ ] No dead external links to docs that have moved (spot-check with `curl -I`)

### Completeness
- [ ] File has an introduction paragraph (what it covers, who it is for)
- [ ] File has at least one example if it describes SQL or CLI usage
- [ ] File ends with a "See also" or "Next steps" section (for user-facing docs)

### Consistency
- [ ] Uses authoritative terminology (see "Terminology ground truth" below)
- [ ] Code blocks specify the language (`sql`, `bash`, `toml`, etc.)
- [ ] Headings use sentence case (not Title Case) — match the majority style

### Duplication
- [ ] Content is not nearly identical to another doc; if overlap exists, one should link to the other

---

## Audience & prerequisites checklist

Apply to every user-facing guide, tutorial, quickstart, and reference page.

### Audience
- [ ] The intended audience is stated or unambiguous within the first paragraph
  (operators configuring a running system / developers building on the extension /
  data engineers writing queries / DBAs doing upgrades)
- [ ] Technical depth matches the stated audience — no unexplained jargon for
  beginner docs, no over-explanation in reference docs
- [ ] Mixed-audience content is split into clearly labelled sections
  (e.g. "For operators" / "For developers") or separated into distinct files
- [ ] The doc does not assume knowledge from a sibling doc unless it links to
  that doc explicitly at the point of assumption

### Prerequisites
- [ ] Hard prerequisites are listed at the top of the file (PostgreSQL version,
  extension version, OS, required CLI tools)
- [ ] Required tools are versioned where it matters
  (e.g. `docker >= 24`, `just >= 1.14`)
- [ ] Extension version required for a feature is noted inline when it differs
  from the current version (e.g. "Available since 0.45.0")
- [ ] Any required PostgreSQL configuration changes are listed
  (`wal_level`, `max_worker_processes`, `shared_preload_libraries`)
- [ ] Privilege requirements are stated (superuser, `pgtrickle` role, etc.)

---

## Rendering checklist

Verify the Markdown renders correctly in the two primary surfaces:
**GitHub** (raw `.md` preview) and **mdBook** (`just build-book` or
`just serve-book`).

### Tables
- [ ] All tables have a header row and at least one separator row (`|---|`)
- [ ] No table cells contain unescaped pipe characters (`|`) — use `\|` or a
  code span
- [ ] Tables are not wider than ~120 characters (GitHub wraps; mdBook clips)

### Code blocks
- [ ] Every code block has a language tag (`sql`, `bash`, `toml`, `rust`, `json`)
- [ ] No code block uses a language tag that mdBook's syntax highlighter does
  not recognise (check against the Highlight.js language list)
- [ ] Inline code spans use single backticks; block code uses triple backticks

### Headings
- [ ] No skipped heading levels (h2 → h4 with no h3 in between)
- [ ] Heading IDs are unique within the file (duplicate headings break anchor
  links)
- [ ] No heading ends with a period

### Lists and nesting
- [ ] Nested list items use consistent indentation (2 or 4 spaces — pick one
  per file)
- [ ] No list item starts with a code span as the first token (mdBook may
  mis-render)

### Admonitions / callouts
- [ ] Callouts use a consistent format (e.g. `> **Note:**`, `> **Warning:**`)
  — do not mix HTML `<div class="warning">` and Markdown blockquotes

### Images and diagrams
- [ ] Image paths are relative and the files exist
- [ ] All `<img>` or `![]()` tags have alt text
- [ ] Mermaid diagrams (if any) render without syntax errors
  (`just build-book` will surface these)

---

## Severity definitions

| Level | Criteria |
|---|---|
| **Critical** | Incorrect information that will cause user errors (wrong function name, wrong GUC, broken example) |
| **Major** | Missing key information, broken link, misleading description |
| **Minor** | Inconsistent terminology, style issue, missing example, awkward phrasing |
| **Gap** | Documented feature is not covered at all |
| **Recommendation** | Structural or architectural suggestion |

---

## Terminology ground truth

Use `docs/GLOSSARY.md` as the primary reference.  The table below captures the
most commonly confused terms:

| Canonical term | Do NOT use |
|---|---|
| stream table | streaming table, streamed table |
| differential refresh | diff refresh, incremental refresh |
| change buffer | change-buffer, changebuffer |
| pgtrickle (schema) | pg_trickle (as schema name) |
| pg_trickle (extension/GUC prefix) | pgtrickle (as extension name) |
| CDC | change-data-capture (spell out only on first use) |
| DVM | differential view maintenance (spell out only on first use) |
| IVM | incremental view maintenance (spell out only on first use) |
| background worker | bgworker, bg_worker |

---

## Common false positives

- `pg_trickle` in GUC names (`pg_trickle.enabled`) — correct, not a schema
- `pgtrickle` in schema references (`pgtrickle.pgt_stream_tables`) — correct
- `pgtrickle_changes` as a schema name — correct for change buffer tables
- Version numbers in `CHANGELOG.md` or `WHATS_NEW.md` that reference old versions — do not flag as stale
- Deprecated features documented in a clearly marked "Deprecated" or "Legacy" section — do not flag as inaccurate

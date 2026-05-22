---
name: improve-documentation
description: "Reviews and improves the pg_trickle documentation in docs/. Checks accuracy against source code, fixes broken links, removes duplication, identifies gaps, and enforces consistency. Use when auditing docs quality, preparing a release, or after significant code changes. Produces a prioritised proposal and optionally writes it to a file."
argument-hint: "Optional scope: a single doc file name or topic (e.g. CDC_MODES.md, scheduler). Omit to review all docs."
---

# Improve Documentation

Full-cycle documentation quality review for the `docs/` directory.  Covers
accuracy, link integrity, consistency, duplication, gaps, and readability.
Ends with a structured proposal that can be written to a file.

## When to Use

- Auditing docs before a release
- After major feature changes (refresh engine, CDC, DVM, scheduling)
- When users report confusing or incorrect docs
- Periodic quality reviews

---

## Procedure

Work through the phases below in order.  Track progress with a checklist.

```
Documentation Review Progress:
- [ ] Phase 1: Inventory
- [ ] Phase 2: Accuracy check
- [ ] Phase 3: Link check
- [ ] Phase 4: Gap analysis
- [ ] Phase 5: Consistency & duplication
- [ ] Phase 6: Readability
- [ ] Phase 6.5: Onboarding path audit
- [ ] Phase 7: Proposal
```

---

### Phase 1 — Inventory

```bash
ls docs/
wc -l docs/*.md docs/**/*.md 2>/dev/null | sort -rn | head -40
```

Read `docs/SUMMARY.md` (table of contents) and `docs/introduction.md` to
understand the intended structure and audience.

For a scoped review, restrict to the requested file or topic.  For a full
review, sample every file: read the first 60 lines of each.

---

### Phase 2 — Accuracy Check

For each doc (or the scoped file), verify claims against the actual codebase.

**Cross-reference these sources:**

| Claim type | Where to verify |
|---|---|
| SQL function signatures | `src/api.rs`, `sql/` |
| GUC names / defaults | `src/config.rs`, `docs/GUC_CATALOG.md` |
| Schema names | `src/lib.rs`, `src/catalog.rs` |
| CDC trigger behaviour | `src/cdc.rs`, `src/wal_decoder.rs` |
| Refresh modes | `src/refresh.rs` |
| DVM operators / rewrites | `src/dvm/` |
| Version numbers | `pg_trickle.control`, `Cargo.toml` |
| Limitations | `src/dvm/parser/validation.rs` |

```bash
# Spot-check a GUC name
grep -n "pg_trickle\." src/config.rs | head -20

# Spot-check a SQL function name
grep -n "#\[pg_extern" src/api.rs | head -20

# Spot-check schema references
grep -rn 'schema = "pgtrickle"' src/ | head -10
```

Flag every factual discrepancy with:
- File and line number in the doc
- What it says
- What the code actually shows

---

### Phase 3 — Link Check

Check all inter-doc Markdown links and any links to source files.

```bash
# Extract all relative links from docs
grep -roh '\[.*\](\([^)]*\))' docs/ | grep -v 'http' | head -60

# Check that linked files exist
python3 - <<'EOF'
import re, os, sys
docs_root = "docs"
errors = []
for root, _, files in os.walk(docs_root):
    for f in files:
        if not f.endswith(".md"):
            continue
        path = os.path.join(root, f)
        with open(path) as fh:
            for i, line in enumerate(fh, 1):
                for m in re.finditer(r'\[.*?\]\(([^)#?]+)', line):
                    target = m.group(1)
                    if target.startswith("http"):
                        continue
                    resolved = os.path.normpath(os.path.join(os.path.dirname(path), target))
                    if not os.path.exists(resolved):
                        errors.append(f"{path}:{i} -> {target}")
if errors:
    print(f"{len(errors)} broken link(s):")
    for e in errors:
        print(" ", e)
else:
    print("All relative links OK")
EOF
```

Flag every broken link with file, line, and the broken target.

---

### Phase 4 — Gap Analysis

Compare what the code supports against what is documented.

**Key areas to check for coverage:**

- Every GUC in `src/config.rs` has an entry in `docs/GUC_CATALOG.md`
- Every SQL function in `src/api.rs` appears in `docs/SQL_REFERENCE.md`
- Every error variant in `src/error.rs` appears in `docs/ERRORS.md`
- CDC modes in `src/cdc.rs` / `src/wal_decoder.rs` match `docs/CDC_MODES.md`
- DVM operators in `src/dvm/operators/` are listed in `docs/DVM_OPERATORS.md`
- DVM rewrite rules match `docs/DVM_REWRITE_RULES.md`
- Limitations in `src/dvm/parser/validation.rs` match `docs/LIMITATIONS.md`

```bash
# Count pg_extern functions
grep -c "#\[pg_extern" src/api.rs

# Count GUC definitions
grep -c "GucSetting\|GucSwitch\|GucString" src/config.rs

# Count error variants
grep -c "^\s*[A-Z][a-zA-Z]*(" src/error.rs
```

Note gaps where documented behaviour differs from or is absent from code.

---

### Phase 5 — Consistency & Duplication

**Terminology consistency** — scan for mixed usage of key terms:

```bash
# Check for mixed capitalisation / naming
grep -rni "stream table\|streamtable\|streaming table" docs/ | head -20
grep -rni "differential refresh\|diff refresh\|incremental refresh" docs/ | head -20
grep -rni "change buffer\|change-buffer\|changebuffer" docs/ | head -20
grep -rni "pg_trickle\|pgtrickle\|pg-trickle" docs/ | head -30
```

Identify which variant is authoritative (use `docs/GLOSSARY.md` and
`src/lib.rs` as ground truth) and flag deviations.

**Duplication detection** — look for near-identical sections across files:

```bash
# Find files with very similar opening paragraphs
head -5 docs/*.md | grep -v "^==>" | sort | uniq -d | head -20
```

Flag content that appears nearly verbatim in more than one place.

---

### Phase 6 — Readability Assessment

For each doc (or the scoped file), note:

- **Missing context**: Does it assume knowledge not linked or explained?
- **Undefined jargon**: Terms used before they are defined (compare to `docs/GLOSSARY.md`)
- **Structural issues**: No introduction, no examples, walls of text
- **Stale examples**: Code snippets that reference functions or options that
  no longer exist
- **Audience mismatch**: Is this for operators, developers, or both? Is it clear?

Do not rewrite yet — just note the issue and the location.

---

### Phase 6.5 — Onboarding Path Audit

Walk the new-user journey from zero to first working stream table.  This is
the highest-leverage UX review because it is the path every user takes exactly
once and can never retry with fresh eyes.

**Entry points to walk (in order):**

1. `docs/QUICKSTART_5MIN.md` — can a user copy-paste every block and succeed?
2. `docs/GETTING_STARTED.md` — does it pick up where the quickstart left off?
3. First tutorial in `docs/tutorials/` — does it assume knowledge not yet introduced?

**For each step, check:**

- **Prerequisites listed upfront**: PostgreSQL version, OS, required tools
  (`psql`, `docker`, `just`, extension version).  Missing prereqs cause silent
  failures that users blame on the extension.
- **Steps are in the right order**: No step depends on an action not yet taken.
- **Each SQL block is runnable as-is**: No placeholder values left without
  clear callouts (e.g. `<your_table>` must be visually distinct).
- **Expected output is shown**: After each command, does the doc show what
  success looks like?  Users don't know if they're on track without it.
- **Error paths are acknowledged**: At least one "if this fails, check X"
  callout per major step.
- **Time estimate is realistic**: If the quickstart promises "5 minutes",
  walk it and time it.  Flag if it is materially longer.
- **Links to next steps**: After completing the quickstart, is it obvious
  where to go next?

```bash
# Check that the extension version mentioned in quickstart matches reality
grep -i 'version\|0\.[0-9]' docs/QUICKSTART_5MIN.md | head -10
cat pg_trickle.control | grep default_version
```

Flag every onboarding friction point with the file, step number, and the
exact user action that would fail or confuse.

---

### Phase 7 — Proposal

Compile all findings into a structured proposal.

**Format:**

```markdown
# Documentation Improvement Proposal
Generated: <date>
Scope: <all docs | specific file/topic>

## Summary
<2–3 sentences: how many issues, severity distribution>

## Critical (breaks user experience)
### C1 — <short title>
- File: docs/FILENAME.md, line N
- Issue: <what is wrong>
- Fix: <concrete action>

## Major (misleads or confuses)
### M1 — <short title>
...

## Minor (polish, consistency)
### m1 — <short title>
...

## Gaps (undocumented features)
### G1 — <feature or GUC name>
- Missing from: docs/FILENAME.md
- Source: src/...

## Recommendations (structural)
### R1 — <title>
- Rationale: ...
- Action: ...
```

After presenting the proposal, ask:

> "Should I write this proposal to a file (e.g. `docs/IMPROVEMENT_PLAN.md`)?"

If the user says yes, write it using the `create_file` tool (not shell echo or
heredoc, to avoid Unicode corruption).

---

## Reference

See [REVIEW_CHECKLIST.md](REVIEW_CHECKLIST.md) for the per-file checklist used
during phases 2–6.5.

See `docs/GLOSSARY.md` for authoritative terminology.
See `AGENTS.md` for coding conventions that affect accuracy checks.

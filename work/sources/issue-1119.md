# Imported source: #1119

Issue: [Preserve downstream consistency across IMMEDIATE refresh and truncate paths](https://github.com/trickle-labs/pg-trickle/issues/1119)
Retrieval: `gh issue view 1119 --repo trickle-labs/pg-trickle --json number,title,body,url,state,updatedAt,labels,comments`
Retrieved at: 2026-09-26T20:23:39Z
Issue updated at: 2026-09-26T20:18:39Z
State: OPEN
Labels: bug
Comments at retrieval: None
Source commit named by the issue: `20c1476b6a8ad4a57cfc5441069f333afc110511`

## Binding issue body

Follow up on #1118 and #1116 to preserve downstream consistency across IMMEDIATE maintenance paths.

Source: the user-approved conversation critique of PR #1118, based on commit 20c1476b6a8ad4a57cfc5441069f333afc110511. This issue preserves that critique's scope; there is no separate disk specification.

## Scope

- Preserve synchronous IMMEDIATE maintenance when full refresh suppresses application triggers. DISABLE TRIGGER USER also suppresses IVM triggers, allowing descendants to miss replacement writes. Preserve maintenance triggers and prior enabled states.
- Capture changes from IMMEDIATE truncate-and-repopulate operations for deferred consumers. The truncate handler bypasses downstream buffer capture, allowing differential descendants to retain removed rows.
- Document recovery of pre-existing broken cascades and include the changed quick_health view in the release migration.

## Evidence and checks

These findings come from source inspection, not executed reproductions:

- Full-refresh trigger suppression:
  https://github.com/trickle-labs/pg-trickle/blob/20c1476b6a8ad4a57cfc5441069f333afc110511/src/api/refresh_ops.rs#L610
- IMMEDIATE truncate handling:
  https://github.com/trickle-labs/pg-trickle/blob/20c1476b6a8ad4a57cfc5441069f333afc110511/src/ivm.rs#L1287

Add focused regression scenarios:
- With an application trigger on the upstream table, full refresh must leave its IMMEDIATE child equal to its defining query before commit.
- For base → IMMEDIATE → DIFFERENTIAL, truncate the base and refresh only the differential child; it must equal its defining query.

Reuse existing maintenance and capture mechanisms. Preserve transaction atomicity and committed changes. Prefer differential maintenance wherever possible. This issue does not authorize broader redesign or a ticket breakdown.

<!-- grove:create-parent-issue source=trickle-labs/pg-trickle:pr-1118-critique -->

## Planning source keys

These keys identify the unmodified issue text above. They add no promises.

- S1: Scope, first bullet, full refresh preserves synchronous IMMEDIATE maintenance and prior trigger states.
- S2: Scope, second bullet, IMMEDIATE truncate-and-repopulate captures downstream changes.
- S3: Scope, third bullet, recovery documentation and release migration for quick_health.
- E1: Evidence and checks, first regression scenario, application trigger plus full refresh and IMMEDIATE child equality before commit.
- E2: Evidence and checks, second regression scenario, base to IMMEDIATE to DIFFERENTIAL after base truncation.
- I1: Final paragraph, reuse existing mechanisms, transaction atomicity, committed changes, and differential maintenance preference.
- X1: Final paragraph, no broader redesign or ticket breakdown.


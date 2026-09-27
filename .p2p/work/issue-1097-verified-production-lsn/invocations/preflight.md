# Delivery preflight: issue #1097

Repository: `/Users/grove/projects/pg-trickle2`
Work item: `work/issue-1097-verified-production-lsn.md`
Starting branch: `main`
Starting commit and comparison base: `6dc7e07c6bf53df2bf2327c4146cb140ca433f64`
Starting worktree: clean; no unrelated tracked or untracked changes.
Current worktree additions: only the canonical contract and retained source snapshots for this delivery. The pre-existing tracked `work/sources/issue-1090.json` remains at its exact starting-commit bytes; the newly fetched parent source is retained separately as `work/sources/issue-1090-parent-source-snapshot.json`.

## Source and agreement

GitHub is configured by `docs/agents/issue-tracker.md`. Issue #1097 was read with `gh issue view --json number,title,body,comments,url,state,parent,subIssues`; issue #1090, #1091, and #1093 sources/comments were also read. No tracker writes were made. #1097 is open, labeled `ready-for-agent`, has no comments, and names #1090 as its parent. #1090 has no comments. #1091 is closed with a proof comment recording 11/11 requirements proven. #1093 is closed with a #PROVEN comment recording 14/14 requirements proven.

Canonical contract: `work/issue-1097-verified-production-lsn.md`, revision v1, exact UTF-8 SHA-256 `3991d8c289b50635066b9b1396ca88190eea4991ba9673d06d558c84cb5105ab`. IDs Q1097-R01 through Q1097-R11 retained. Exact #1090 parent contract snapshot SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`. No unresolved planning gaps or decisions. `docs/retrospective-learnings.md` is absent.

Binding inputs resolved by `p2p_filesystem.py`:

- `work/sources/issue-1097.json` — `013f52ebc05d3766582a87d6ba23af51af2fc84c481abb28431584ae5be4210d`
- `work/sources/issue-1090-parent-source-snapshot.json` — `c65c99edb02da9d1640f3dff9189a971a2bd3414956af30ceccf273c536bab06`
- `work/sources/issue-1090-parent-contract-v1.md` — `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`
- `work/sources/issue-1091-prerequisite-snapshot.json` — `9e457f926f0593fc8960373c649607fe16cdd523411b2cefef83fa898ac44ef9`
- `work/sources/issue-1093-prerequisite-snapshot.json` — `2610c30baa257b13b1884472d059349f719c531c2af186b696c4e5d12de5fa85`

## Host and storage checks

Host: Codex collaboration agent contexts. Installed stage skill files are readable. A separate harmless read-only probe context completed before implementation as `/root/delivery_capability_probe`; it read the repository, observed starting commit `6dc7e07c6bf53df2bf2327c4146cb140ca433f64`, and confirmed `.p2p/work` was visible. The agent host exposes its canonical agent identity, not a session UUID. The probe task made no writes.

The root context ran `p2p_filesystem.py setup`; existing `/.p2p/tmp/` ignore was already present. Before implementation, an actual disposable file was written, read back byte-for-byte, and removed under `.p2p/work/issue-1097-verified-production-lsn/`; result: `artifact write/read/remove verified`. Destination: `.p2p/work/issue-1097-verified-production-lsn/`.

Planner stage: `/root/plan_issue_1097`, invoked in a separate collaboration agent context with repository workspace-write available and task scope limited to canonical contract planning. It read the source snapshots, wrote and reread the contract, and returned the exact contract SHA above with no gaps. The canonical contract is the planner output.

Access to fixed candidate and comparison base will be checked again from each independent review/proof context after capture. Those contexts will be instructed to keep candidate, base, contract, and source inputs read-only and use only disposable scratch for checks.

## Scope correction

An initial parent fetch targeted an existing tracked source path. Before implementation, the fetched bytes were copied to the unique parent-source snapshot path and the tracked path was restored from the recorded starting commit. `git diff -- work/sources/issue-1090.json` is empty; the fetched copy hash is retained in the binding inputs above.

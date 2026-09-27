# Proof invocation record

Work item: work/issue-1097-verified-production-lsn.md, revision v1, SHA-256 dae07ec8c073477626add9b70a114891b5bbe7431e9790537c075bd731cc4259.
Candidate: snapshot:sha256:361e2afe069e25ceb27988a01809dd9f840150e5e86307d48e422be6dce3bac9.
Comparison base: 2dfe8dae90452120cf6760b71dd0def8c241756b.

Host invocation: collaboration.spawn_agent, task identity /root/prove_issue_1097_final. This was a distinct agent context from the reviewer and implementation. The host exposed the canonical agent identity, but no separate session UUID.

The stage was instructed to keep the candidate, base, contract, and source inputs read-only and to use disposable scratch for proof runs. Its inherited workspace permission was workspace-write rather than an enforced read-only sandbox. The proof context wrote only under /private/tmp; it reported no changes to candidate product files or repository .p2p files. The enclosing agent copied the returned E2E, verifier, candidate-binding, installation, and CI-control evidence into the issue evidence directory using the P2P helper.

Independent checks in scratch: the exact snapshot manifest was recomputed with all 1,566 entries matching; pinned Verus reported 44 verified and zero errors and rejected the semantic mutation; all four full LSN package E2E cases passed; all three CI enforcement controls exited 1 for the expected diagnostics. The fresh review report was inspected for Q1097-R10.

Result: PROVEN, 11/11 requirements. Report SHA-256 d32594900338d30a5ffc17ccc375f27d96d139139aacdf4d887bdf148ebcbfbd; 10,106 bytes.

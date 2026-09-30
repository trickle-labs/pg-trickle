# Archived P2P records

The implementation for issue #1096 was published in PR #1117 and is merged.
The canonical contract, candidate, review, proof, and source snapshots remain in
the checkout. This archive moves bulky evidence and superseded stage records to
Git history; it does not change their historical verdicts.

Archive source commit: `7e7d574dc80d1d793f325ccaebf8703375296e4e`

Every entry below was verified in that reachable commit. The mode and blob ID
are its Git tree identity. Recover files for inspection into a temporary
directory with:

```sh
git archive 7e7d574dc80d1d793f325ccaebf8703375296e4e \
  .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/ci-repair.md \
  .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence \
  .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/history \
  .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/implementation.md \
  .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/invocations \
  .p2p/work/issue-1096-real-publisher-to-subscriber-recovery/repair.md \
  | tar -x -C /tmp
```

Access depends on the archive source commit remaining reachable in this
repository's Git history. This reduces checkout size only; it does not remove
the data from Git history. Restore required evidence before resuming verification.

| Mode | Blob | Archived path |
|---|---|---|
| `100644` | `beb388a76ae7e263722acebf138da4de41fe34a3` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/ci-repair.md` |
| `100644` | `e7aeac448d627ec43a8185c13c2e01a89d8d4612` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/comparison-base-edf700ef.tar.gz` |
| `100644` | `a4247612359c614354dd40c3c4c320b0fff83c7e` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/final-candidate-reconstruction.log` |
| `100644` | `55137b4adaf85c7e2ce0b764bb67986524e956cf` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/final-machine-format-attestations.json` |
| `100644` | `8e29ebc87fbd8aba802667614c5483f67950f0f9` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/final-machine-format-e2e.log` |
| `100644` | `454a2c10044b1d05cd3ee5895196467c27afcb35` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/final-public-issue.json` |
| `100644` | `c41a09edfc3db211d83a4d62e1061ff9e6d61258` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/fmt-lint.log` |
| `100644` | `84cbbc8aa4da87c1b01b0a1ede88f064b19b348a` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/native-proof-attestation-final.json` |
| `100644` | `c370e23d868564a3af2ae77e1c8404b82c9288ab` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/native-proof-run-final.log` |
| `100644` | `e742239a2fb77274c0bad4ba10beef06410fddb0` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/postgres18-candidate-image-id.txt` |
| `100644` | `d7b25f5d2e35305de605a94cb0cadf71f3b00089` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/pr1117-publication-recovery-installation.json` |
| `100644` | `bf030c15e30b2e766f08c6f26fd91c3d60c5f628` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/pr1117-publication-recovery-job.log` |
| `100644` | `858f87634a1ce83a141b55d1833c606bd77d8e3d` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/pr1117-publication-recovery.log` |
| `100644` | `ca7575958d8d7122b4a512f7a5917e0a3da19628` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/pr1117-release-evidence.json` |
| `100644` | `9d99608dc55220747f8aa9049b6ee826dc91aa15` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/release-qualification-gate.log` |
| `100644` | `b212b042583024722334fd056c3458178e110c2e` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/repair-machine-format-attestations.json` |
| `100644` | `3173254e4e5b2accbdfeceb15e60334e0ef6f2e1` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/evidence/repair-machine-format-e2e.log` |
| `100644` | `b9a12ecc77eb07879db40e9a42b017b99094ac65` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/history/5411261abfa17a5e6c5bff36f88f45d90d5ec2bd897373167223192a4ab13d5d/proof.md` |
| `100644` | `2d39ebfe34f957714fe2f61c5e5d99e7662b59c0` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/history/7b70e5c500d503b9a245d1c7eed08b59b7f1cde68a214dc2432087e0c6e63887/ci-repair.md` |
| `100644` | `2f79b924ba8881e9cfafb56d78a33cf5ae37bfc9` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/history/90f827a25cbae99c10fe67fb8aa034dcfb00ff49cdbd87e17b6b2612b5412f81/review.md` |
| `100644` | `67d94af17330a665394ea08151242ec80829bdce` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/history/b864a3d7f3b7687c5e54aef7803e5bc32f9b09e07090b2f29af5bcd2194f510f/proof.md` |
| `100644` | `32688dcbbd6c9db7bc6e35de5f97b4a43849c917` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/history/e300048623e660f02317b1a679f2a442095141daa5298514dce7361070bb4616/candidate.json` |
| `100644` | `14533bc78a1b587639bc8b009982fe075c999508` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/implementation.md` |
| `100644` | `2be81481c7aba68d52cdaa93c4d7ab037c6f1388` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/invocations/prove-final.md` |
| `100644` | `c5afae8545d110ad6adb6a79dbce919d4651c17d` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/invocations/review-implementation-final.md` |
| `100644` | `a55e37f63f3e66b2e0ada56edbdad1f1e6363005` | `.p2p/work/issue-1096-real-publisher-to-subscriber-recovery/repair.md` |

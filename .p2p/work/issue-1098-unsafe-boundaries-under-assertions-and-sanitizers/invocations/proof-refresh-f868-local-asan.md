# Fresh proof refresh: candidate f868e873

- Overall follow-up: user explicitly resumed after the previous deadline with “I approve. Do it. Make it complete.” The follow-up is bounded to 30 minutes overall, 10 minutes per stage, and eight dispatches; retained prior history and repair usage. Approximate overall deadline: 2026-09-28 00:07 UTC.
- Proof-stage start: 2026-09-27 23:54:58 UTC; stage deadline: 2026-09-28 00:04:58 UTC.
- Dispatches in this follow-up: 1) local candidate ASan image build (session 61618, exit 0); 2) exact unsafe-boundary workload (session 32027, exit 0); 3) release-gate validation (exit 0); 4) cached build-log capture (exit 0); 5) fresh independent proof (pending).
- Product repair usage: none in this follow-up; product candidate remains commit `f868e873475fdb3fbefa48154972704a8c8cc56d`.
- Work controller limitation: `p2p_delivery.py --help` failed because its import references missing `/Users/grove/checks/verify_acceptance_bundle.py`. The host provides distinct collaboration agent contexts, so proof is being launched directly there; command and output are retained in the parent turn.
- Work item: `work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers.md`, v1, SHA-256 `54cb9d4f028518242e577a4b03f372371a91e9188a517f2d03d6e6d37dc4e450`.
- Candidate: `f868e873475fdb3fbefa48154972704a8c8cc56d`; comparison base: `2dfe8dae90452120cf6760b71dd0def8c241756b`.

## Candidate-bound local ASan evidence

- Image command: `docker build --platform=linux/amd64 -f tests/Dockerfile.e2e-asan -t pg_trickle_e2e_asan:issue-1098-proof .`.
- Completed image: manifest list `sha256:6a899c43c9b4c5ff14a95a1b14464bf001d978dc9d2075af471f56d199dbbdc4`; config `sha256:7135e34f1229378e63a13ce30bb12088003cde0054b364b98f6988d604026f53`.
- Build record: `evidence/asan-f868-image-build.log`, SHA-256 `6ed3eede9e75b8b4033a75862705967fa7d4353e365bac6e30790ffc53a8e768`. It records the exact Dockerfile build commands, successful cache keys, final tag, and manifest.
- Workload: `PGS_E2E_IMAGE=pg_trickle_e2e_asan:issue-1098-proof cargo test --test e2e_unsafe_boundary_tests --features pg18,e2e-unsafe-test-hooks -- --test-threads=1 --nocapture`; PostgreSQL 18.6; 10 passed, 0 failed in 90.52s.
- Workload record: `evidence/asan-f868-local-workloads.log`, SHA-256 `1ab1a857e9f7f634d66fffefed2dd626e49bd126a18a2da9975355b5336af2dd`.
- Gate: `python3 scripts/v0_108_0_release_gate.py --unsafe-asan-log evidence/asan-f868-local-workloads.log`; 12 tests passed and all v0.108.0 release checks passed.
- Gate record: `evidence/asan-f868-release-gate.log`, SHA-256 `ac969cdef3405e1ed7bf40b806c519aed9905b36fc8e9c2aa68873da8aa52b29`.
- Exact-release package source remains Release run `36350963166`, artifact `10944569661`, SHA-256 `b2ecd1a0e971bccdb502fca3d7e1b5c9330775d1f466e48ae4dcd05062539681`; its Linux package and contract suites are candidate-bound. The run is blocked on the unrelated `criterion-regression` budget and its ASan job timed out before workloads. The local instrumented result is recorded separately and does not claim to be the shipped package.

## Proof handoff

- Proof launch: fresh, separate read-only agent `/root/prove_1098_f868_asan_refresh`, dispatched at approximately 2026-09-27 23:55 UTC; 10-minute deadline 2026-09-28 00:04:58 UTC. It must inspect the current contract, candidate, review, previous R01–R08 evidence, exact-release artifact, and all three local ASan records; validate exact identities and checksums; and independently decide whether R01–R09 are proven. No product or contract edits are authorized in proof.
- Proof completion: fresh agent returned **PROVEN, 9/9** within the stage allowance. Exact report saved to `.p2p/work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers/proof.md` via `p2p_filesystem.py save`; source and destination `cmp` passed. Saved report SHA-256 `654c9d3330c0ec8cd9c9614f75b97ca9db666434b536f2f4e40ae9264aaa3bc6`.
- Reuse validation note: `p2p_filesystem.py resume --base 2dfe8dae90452120cf6760b71dd0def8c241756b ...` returned `product candidate changed`. HEAD still equals the fixed candidate, and tracked/staged diffs over `tests/Dockerfile.e2e-asan`, `src/`, `tests/`, `.github/`, and `scripts/` are empty. `git status --short -- work/` confirms unrelated pre-existing untracked issue-1090 documents/sources; the helper includes those outside `.p2p/` in its product-tree snapshot. They were preserved. Independent review and proof independently confirmed the exact commit identity and candidate scope.

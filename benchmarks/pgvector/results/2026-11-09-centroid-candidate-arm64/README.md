# Centroid recovery and drift against an earlier candidate

These local Linux ARM64 runs used the v0.109.0 qualification artifact at
commit `fe2904bfae6e5031cf5df37cd53a2613b6ea92d3`, archive SHA-256
`370e325e65bd8f991f522c2ac90dac7d2bb506a2727956fb1240524d58ab366b`.
The normalized archive payload contained 157 files with digest
`79427ce24042f9f96146ece788685f2f19ff485cf84d578f8204a408506cc8d3`.
For all 16 test-container observations, the installed file list and digest
matched that payload, and the observation recorded the candidate commit and
archive digest. PostgreSQL was 18.6 with `fsync`, `synchronous_commit` and
`full_page_writes` on. The host was an Apple M1 Pro; the candidate images were
`sha256:ab036388b84584de079bdabdaf27da0712ac27ecc01db0a9781f67c653762935`
for pgvector 0.8.1 and
`sha256:e84deda6329086aeb490e1f69e6a2986f524cd4ee7a502bb6478c0c763ebdec4`
for 0.8.7. The images' `vector.control` defaults identified those versions;
the drift logs also report the installed versions.

Six recovery cases passed separately on [0.8.1](recovery-081.log) and
[0.8.7](recovery-087.log): committed source and rolled-back refresh across
container kill/restart; missing state; corrupt state; concurrent source
writer serialization; repeated large rewrite/rebase suppression across
restart; and numerically unsafe state remaining unbound. The six individual
installation observations are preserved for [0.8.1](recovery-081-attestations.json.gz)
and [0.8.7](recovery-087-attestations.json.gz).

The 160-call randomized regression passed on [0.8.1](short-081.log) and
[0.8.7](short-087.log), with separate installation observations
([0.8.1](short-081-attestation.json.gz), [0.8.7](short-087-attestation.json.gz)).
The opt-in 2,000-call drift test also passed on [0.8.1](drift-081.log) and
[0.8.7](drift-087.log), with separate observations
([0.8.1](drift-081-attestation.json.gz), [0.8.7](drift-087-attestation.json.gz)).
Both long runs reported zero maximum component error at float32 output
precision under the test's absolute-plus-relative bound. They used one seed,
three dimensions and a small dataset; 1,962 algebraic history observations
include repeated observations after no-op mutations and are not distinct
refresh counts.

These direct-suite runs are not the machine-enforced final release report.
The package predates the integrated candidate and its operator files. They do
not establish a broad numerical or performance envelope, natural long-term
rebase behavior, or selector calibration.

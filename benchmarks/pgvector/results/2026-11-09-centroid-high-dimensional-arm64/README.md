# High-dimensional centroid drift smoke runs

On Linux ARM64, the v0.109.0 candidate archive for commit
`1e291536bf0380b37a509b730155ca3d3a0ccea4` (SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`)
passed the 2,000-call seeded churn test with 1,536-dimensional vectors on
pgvector 0.8.1 and 0.8.7. Every group and component was compared with a fresh
native AVG after each call using `1e-5 + 1e-6 * abs(native_component)`.
Both logs report zero maximum component error at float32 output precision.
Each reports 1,962 observations of the algebraic history entry; repeated
observations after no-op mutations are not distinct refresh counts. The
160-call three-dimensional regression also passed on 0.8.1.

The host was an Apple M1 Pro and the containers used PostgreSQL 18.6. The
test exercises one seed, five groups and a small source, with repeating
three-value component patterns. These results do not establish safety for
diverse high-dimensional inputs, large groups, extreme values, joins or
concurrent workloads, and do not calibrate the strategy selector.

The test images contained the earlier `1e291536` package, but the automatic
attestation was mistakenly supplied metadata and a payload path for the later
`c138e4fe` archive. Both [original observations](mismatched-attestation-081.json.gz)
([0.8.7](mismatched-attestation-087.json.gz)) correctly show that mismatch; their
candidate metadata must not be used to identify the installed package. Their
157 installed files match the candidate file list in the retained
[earlier archive attestation](reference-1e-attestations.json.gz). A second
[observation](attestation-081-correct.json.gz), taken while the 0.8.1 test was
running with the correct metadata and payload, also matches all 157 files.
The evidence is for the earlier candidate, not the final integrated release.

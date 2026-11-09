# Combined-branch smoke test

The locally assembled commit `3528f1fa2aa776ce44a92a9c4c2368c031a86421`
combines the package, supervisor, RAG, subscription, and centroid branches,
including the corrected upgrade rehearsal. Its v0.109 release-contract gate,
both 0.108.3 and 0.108.4 upgrade-completeness checks, `just fmt`, and
`just lint` passed. The [test log](run.log) records six passing tests:
subscription controls, restricted-tenant RAG, 1536-dimensional RAG setup,
supervisor request filtering and operation, and the 2,000-call centroid drift
case.

The tests ran on an Apple M1 Pro using a Linux ARM64 development image with
PostgreSQL 18.6, pgvector 0.8.7, and pg_trickle 0.109.0. Its image ID was
`sha256:bc4a6094c8c84a38acb7c56ef4663be620067cedccd4f6fc5bbf67efbcb6ff8c`
and library SHA-256 was
`de9d0d7e81f058ad67014a70aded4119733cf23990fb3d2def33b3b2e24ced6f`.
The image was built from the supervisor branch; its extension sources,
manifests, control file, and migration SQL matched this combined commit.
The test image is not a release archive and does not attest the final
candidate. This is a limited interaction smoke test, not completion of the
release, workload, recovery, or performance qualification.

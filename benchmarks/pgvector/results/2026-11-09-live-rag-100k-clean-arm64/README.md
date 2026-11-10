# Fresh 100,000-document live RAG run

This uninterrupted three-cycle mixed-churn run used PostgreSQL 18.6,
pgvector 0.8.7 and the v0.109.0 Linux ARM64 archive at commit
`1e291536bf0380b37a509b730155ca3d3a0ccea4`, SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The benchmark source was `117e8296c2bba7ce42d1b311817e28564f41e73f`.
The [installation observation](installation.json.gz) matched all 157 archive
payload files. The runner itself records package identity as operator-declared;
the separate installation observation is the evidence for the installed files.

The host was an Apple M1 Pro using Linux ARM64 Docker. The container used
image `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`,
1 GiB shared memory, 10 available CPUs and 8,124,516 KiB host memory reported
inside the container. This fresh database used the normal benchmark command,
with 100,000 initial documents, 384 dimensions, three batches of 12 mixed
update/visibility/delete/insert slots, tenant 2, and no artificial generation
delay. The hourly fixture schedulers were not paused. There was no controlled
concurrent traffic; background activity was not excluded.

Initial setup took 1,555.2 seconds. Its initial differential population
reported a FULL fallback after exceeding the change-ratio threshold. All
three corpora matched their defining SQL initially and after every mixed
cycle. Following each source commit the six relevant embeddings were not yet
eligible; after generation all six were eligible and none were yet present in
the guarded current views. After each approach's maintenance, all six were
visible in its guarded view.

The three serial samples took 0.128–0.136 seconds for application maintenance,
5.492–5.619 seconds for differential refresh, and 482.642–547.128 seconds
for full recomputation. Generation took 0.409–0.438 seconds and
commit-to-eligible time took 0.481–0.517 seconds. All three batch differential
refreshes completed without FULL fallback using `explicit_dml_reconciled`.
The maintenance order rotated between cycles; eligible-to-searchable times
therefore include earlier serial approaches and are not independent service
latencies. Three samples do not support meaningful tail percentiles or a
general performance ranking.

Vector and hybrid recall were 1.0 with no underfill in these samples. Normal
planner runs selected no ANN index for any approach; forced eligibility used
the differential ANN index but not the other two. These results therefore do
not qualify ANN retrieval quality. The [complete report](results.json.gz)
contains plans, individual measurements, relation sizes and cluster-wide WAL
movement; the [log](run.log) records refresh notices. The run lacks controlled
concurrent writes, delay distributions, recovery scenarios and a final-candidate
package. It is not a release sign-off or a general latency claim.

The command follows [LIVE_RAG.md](../../LIVE_RAG.md), using a disposable
container with `--shm-size=1g` and `run_live_rag_batches.py --rows 100000
--dimensions 384 --batch 12 --cycles 3 --tenant 2 --workload mixed`, plus the
archive commit and SHA-256 above. Raw report SHA-256:
`f706a9d1037246b3490df08d62f4c1edbdd8da0bcd79f5b74620caa0fdac31ca`;
log SHA-256: `ae90613264bbb63601558780b924e4d6c88337bf29a700b762d03dd697cc149a`.

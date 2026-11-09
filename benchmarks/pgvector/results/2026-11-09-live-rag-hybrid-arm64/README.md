# Hybrid recall measurement smoke tests

The live RAG recall command was extended from commit `c4f7e965` to compare
the normal hybrid-fusion result with the same query using exact vector
candidates, in one snapshot. These tests use PostgreSQL 18.6 on Linux ARM64
Docker on an Apple M1 Pro, with development images, not the final candidate
artifact. They verify functional behavior, not performance or large-scale
retrieval quality.

- [pgvector 0.8.1](pgvector-081.log) and [0.8.7](pgvector-087.log): the three
  live RAG tests, including a restricted reader, an empty unauthorized tenant,
  and a 384-dimensional vector.
- pgvector 0.8.7 at [768](dim-768.log) and [1536](dim-1536.log) dimensions:
  the wide-vector setup and measurement test.

The development image IDs were
`sha256:620d26b64447b4dce1960f8fca396b6f4b2b985763cb4f75cb67d42d45261212`
for 0.8.1 and
`sha256:bc4a6094c8c84a38acb7c56ef4663be620067cedccd4f6fc5bbf67efbcb6ff8c`
for 0.8.7.

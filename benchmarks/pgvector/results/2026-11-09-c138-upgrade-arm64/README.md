# Published package upgrade rehearsal

On Linux ARM64, the published v0.108.3 package was upgraded directly to the
v0.109.0 qualification archive built from commit
`c138e4fe10cb07774129eba1d05a54b6634805b1`.

- Published archive SHA-256: `498bbe3bc7415a22389d767c0b0fae4d96aa0618c7e64704c500af934835cf99`
- Candidate archive SHA-256: `d4506e5f91c4f3e2d5210aa9d30878cf20f0443e2ae7752e939ad40b64ed2833`
- Candidate and installed payload digest: `73f75eccf0099d42023385de286da74230b839993e983990bff783b03b314961`

The rehearsal passed with pending changes and durable consumer data preserved;
it also checked WAL state, rollback, and committed drain. The attestation records
157 candidate files and the same 157 installed files. The log and compressed
attestation are included here. This is evidence for the specified archived
commit and platform, not qualification of a later candidate.

# Final candidate identity validation

Command:

```sh
python3 /Users/grove/.agents/skills/publish-pr/scripts/p2p_filesystem.py --repo /Users/grove/projects/pg-trickle3 validate --base 2dfe8dae90452120cf6760b71dd0def8c241756b work/issue-1119-immediate-downstream-consistency.md
```

Result: exit status 0.

The validator confirmed that the exact work-item hash, comparison base, and binding input hashes match `candidate.json`; the snapshot digest recomputes to `snapshot:sha256:08f36577aca0522020d99d2bad996917b98746f08d0875bb06fb3596176405c9`; and the current product tree matches all 1,556 manifest entries, including paths, bytes, and modes. The index was empty. The full JSON output contains the complete base64 manifest and was not retained; the candidate file and this concise result record are durable.

Verification environment: macOS checkout at `/Users/grove/projects/pg-trickle3`.

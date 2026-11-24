# Recovery and WAL-retirement suite fixes

After the selector and harness fixes, the release-suite runner executed both
suites on Linux amd64 under emulation on an Apple M1 Pro. The candidate was
the v0.109.0 archive at commit `1e291536bf0380b37a509b730155ca3d3a0ccea4`,
SHA-256 `5a99cf20e5134ae5749196b95b74c9321782b947e2e8458e0ae004e5c2eed1fd`.
The temporary local contract differed only in its Docker image tag, which
pointed to the image built from that archive. Both suites ran concurrently;
their durations are not performance measurements.

The [recovery suite](recovery.json.gz) passed all 17 required cases, including
the four scheduler cases omitted in the failed hosted run. All 17 recorded
installation observations matched the candidate payload. The
[WAL-retirement suite](wal-retirement.json.gz) passed all eight required cases
using the full isolated-container harness; all nine observations matched the
candidate payload. The full machine-readable case events are in the compressed
logs. These runs validate the corrected selection and harness against an
earlier candidate; final release qualification remains required.

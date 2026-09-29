# Checked LSN specification and reviewer handoff

## Scope

`verification/lsn.rs` is the production source imported by `src/lib.rs`. The
verified executable core accepts byte strings with exactly one `/`, one to
eight ASCII hexadecimal digits on each side, and no other bytes. It returns
`high * 2^32 + low` as an unsigned 64-bit position. The canonical formatter
emits an uppercase, minimally padded high half, `/`, and exactly eight
uppercase low-half digits. It returns the exact bytes that the Rust wrapper
converts to `String`.

The parser's `&str::as_bytes` adapter relies on Rust's valid UTF-8 invariant;
grammar validation and numeric conversion happen in the checked byte parser.
The formatter's `String::from_utf8_unchecked` adapter relies on the verified
ASCII postcondition of `format_bytes`; allocation and UTF-8 construction are
outside the Verus model. The scope excludes PostgreSQL WAL durability,
transaction scheduling, and whole-extension correctness.

## Definitions and obligations

`valid_lsn_bytes` is the grammar predicate. `hex_value` gives the base-16
mathematical value of a valid component. `lsn_bytes_value` gives the combined
position. The executable parser carries postconditions against those
definitions, including rejection of byte strings outside the grammar.
`pack_halves` states the unsigned high/low combination. `numeric_gt` and
`numeric_gte` compare parsed positions. `format_bytes` returns the bytes used
by the production `format()` adapter and specifies uppercase hex, exactly one
separator, an eight-digit low half, and parser round-trip identity.

The CI runner requires the named obligations
`lsn_parse_value_and_bounds`, `lsn_numeric_order`, and
`lsn_format_parse_roundtrip`, captures the exact pinned Verus output, and
requires a semantic mutation of the executable `pack_halves` expression to
fail verification.

## Toolchain and evidence

The verifier image is pinned in `scripts/check_lsn_verification.py` to
`ghcr.io/verus-lang/verus:0.2025.06.23.2e59154@sha256:c4d0471379b23c3c6f52e3d7226c7dad28f488c4288e14c623002bd279a745e0`.
The exact invocation omits the unsupported `--verify` flag and runs
`verus --triggers-mode silent verification/lsn.rs`. CI fails closed if the
baseline exits unsuccessfully, omits a zero-error verification summary, reports
fewer than the required obligations, or accepts the semantic mutation.

## Caller inventory

- `src/version.rs`: LSN parse, formatting, ordering, and frontier merge.
- `src/scheduler/watermark.rs` and `src/scheduler/mod.rs`: coordinator and
  scheduled watermark conversions and persisted frontier validation.
- `src/cdc/mod.rs`: CDC writer-fence holdback and transition ordering.
- `src/api/recovery.rs` and `src/api/refresh_ops.rs`: public recovery and
  refresh validation at durable frontier boundaries.
- `src/wal_decoder.rs`: WAL transition comparison and numeric conversion.

These callers use the checked parser and numeric helpers. Invalid persisted
frontiers return errors or refuse progress before mutating committed state.

## Independent review status

Independent specification review: **pending**. The implementation stage has
not performed or claimed the independent review required by Q1097-R10. The
reviewer should check the grammar against PostgreSQL 18, the mathematical
definitions against the executable postconditions, the UTF-8/allocation trust
boundary, and the caller inventory. Record reviewer identity, source digest,
findings, and disposition in the separate review artifact before making a
verified LSN claim.

## Known verification limits

The pinned Verus command and mutation control must run successfully before the
obligations can be claimed as proven. Database runtime coverage is separate:
this artifact does not establish scheduler, CDC holdback, or WAL transition
behavior. Candidate package and installed-library identity must be bound by
`scripts/check_lsn_candidate_binding.py` and a real packaged runtime attestation.

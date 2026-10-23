# Bounded scalar identity framing

The admitted wire slice is exactly one bool, int2, int4, int8 or UUID field,
including typed NULL, in an existing V2 or V3 identity and any of the eight
existing domains. This covers common single-column source keys. Other field
types, empty tuples and multi-field tuples keep their existing ordinary
encoder/validator contract. The separate checked capacity function covers the
ordinary 1024-field / 1 MiB tuple bound for every field family. This is not a
proof of all PostgreSQL identities or scalar canonicalization.

## Independent wire grammar

`scalar_wire` describes a complete byte sequence independently of the encoder:

| Offset | Meaning |
| --- | --- |
| 0 | Version 2 or 3 |
| 1 | Domain tag 1 through 8 |
| 2–5 | Big-endian field count, exactly `00 00 00 01` |
| 6 | Type tag 1/2/3/4/13: bool/int2/int4/int8/UUID |
| 7 | State 0 for NULL or 1 for a value |
| 8 onward | No payload for NULL; otherwise exactly 1/2/4/8/16 payload bytes |
| Final byte | Tuple terminator `ff`, with no following input |

The complete frame is 9–25 bytes. A non-NULL bool payload must be 0 or 1.
All other admitted fixed-width payload byte patterns are allowed by framing;
the integer sign transform and PostgreSQL datum conversion remain external.
Embedded zero and ff bytes are ordinary fixed-width payload bytes, not field
delimiters. NULL and a value have distinct state tags even when a value's
payload is all zeroes. Version, domain and type remain explicit independent
discriminators; no payload hash substitutes for them.

`represents` relates an entire grammatical frame to supplied version, domain,
type and optional payload bytes. `unique_scalar_decoding` proves that two such
interpretations of the same frame must agree on every input, including NULL
state and exact payload. It establishes unambiguous boundaries and domain/type/
version separation. No executable decoder is introduced by this slice.

## Executable contracts and production adapters

Production imports `verification/identity_frame.rs` directly. Its width lookup
has an exact specification and returns zero for unadmitted tags. The encoder
has no unchecked preconditions: it validates version, domain, type, width and
bool payload before indexing or allocating. It returns Some exactly for
valid inputs, and its postcondition relates those bytes to the independent
grammar and input payload. Loop bounds prove safe copying and a maximum
25-byte output. The validator likewise has no preconditions and returns true
if and only if the entire supplied slice follows the grammar. Together these
contracts establish the encoder/validator relationship, full consumption and
exact recovery of the supplied input interpretation.

`checked_frame_size` accepts exactly `7 + 2*field_count + payload_bytes` when
the field count is at most 1024 and the complete size is at most 1 MiB. It
rejects inputs before arithmetic when their bounds cannot fit; executable
overflow and underflow are verified. The ordinary framer still sums actual
payload lengths with checked addition, then calls this exact implementation.
Header/tag/terminator overhead is included. The verification driver checks
that the ordinary declared limits still agree with this contract.

Both public pure tuple encoders enter the checked scalar encoder only for an
actual one-field slice and an admitted tag. Invalid admitted payloads return
the existing RowIdV2Error boundary instead of escaping to the ordinary path.
The SQL record adapter still validates PostgreSQL type/record metadata and
encodes datums through the existing registry. It now delegates framing to the
same pure framer, removing its duplicate framing loop. Single-field records
use a stack metadata slice; multi-field records borrow their already-owned
payloads without copying payload bytes. The ordinary V2 validator preserves
its V2-only version contract and delegates actual one-field admitted frames
to the checked validator. V3's constructor uses the same checked encoder;
this change does not expand the public V2 validator's accepted versions.

## Compatibility and qualification

Valid V2/V3 bytes and domain/type tags remain immutable. The existing
`scan_int4_minus_one` fixture omitted its separate ff terminator; full fixture
validation now reproduces that error and the fixture is corrected to match
the already-passing immutable predecessor installed case. This corrects test
data without changing the wire format. Every V2 golden frame is now validated,
and the corrected vector is compared to the actual pure encoder.
Malformed bool payloads that previously passed the pure constructor are now
rejected, while width validation is retained. PostgreSQL's existing typed
scalar conversion already produces valid payloads. No stored identities
are rewritten and no probe format changes. If an external integration
persisted malformed identities, it needs the existing protected FULL recovery
path; this patch does not reinterpret those bytes as valid identities.

The pinned compatible verifier requires all five named executable/uniqueness
obligations, a zero-error baseline and five intended semantic failures: lost
terminator, accepted trailing bytes, invalid bool, lost domain validation and
omitted payload capacity. Normal builds and pure tests cover typed NULLs,
zero/ff payloads, both versions and every admitted domain/type, every proper
truncation, trailing bytes, malformed headers/states/tags, exact capacity
limits and usize::MAX arithmetic inputs. Existing independent golden vectors
and constructor boundary tests run without a database.

Installed PostgreSQL qualification checks literal predecessor-compatible
V2/V3 frames for all five admitted types and selected domains. A separate
dedicated-container seam replaces only the probe function with one constant
before expression indexes are created. Scan and inner-join streams then
refresh simultaneous updates, deletions and insertions while all probes
collide. Exact public bags/schemas, distinct complete stored identities,
fresh DIFFERENTIAL history and absence of FULL fallback are required.
Named consumers are the keyed MERGE matcher in `refresh/codegen.rs` and the
pre-change keyed anti-join in `dvm/operators/join_common.rs`: both compare
complete identities after matching probes. Probe collisions therefore do
not authorize identity equality in these selected native tests. SQL operator
resolution and PostgreSQL execution remain ordinary conformance boundaries.

Both installed cases already pass on the immutable predecessor
`d84139858410627102795f493e6436c2ac943eef`; final candidate qualification and
independent review are recorded separately once complete. No pending runtime
or remote CI run is reported as passing.

Candidate binding hashes the checked source, ordinary adapters, named consumer
sources, golden fixtures and test cases, and compares packaged/installed
library bytes with the declared frozen commit. It trusts the runner for build
origin and successful named execution. The verifier, solver, compiler, standard
library contracts, allocation success, pgrx/FFI, PostgreSQL type metadata,
scalar canonicalization, numeric special values, collation, tuple capture,
SQL lowering/operator lookup, transaction protocols and probe hashing remain
outside the framing theorem. No hash uniqueness claim is made.

# Row identity V2 wire format

This document is the normative wire-format contract carried by v0.87.17. A change to
any assigned byte, width, limit, or algorithm requires a new identity version.
The row identity is opaque. v0.87.15 does not change V1 storage or make V1 and
V2 state interoperable.

## Scope and versions

The encoder targets PostgreSQL 18.x. The registry accepts PostgreSQL major
version `18` only. An unknown major, type contract, collation, or operator class
is rejected before identity state is created.

The format uses these fixed versions:

| Item | Value | Encoding |
|---|---:|---|
| Identity version | `2` | `0x02` |
| Probe version | `1` | Algorithm identifier; not emitted in probe bytes |
| Integer byte order | big-endian | most significant byte first |
| XXH3 seed | `0x9E3779B185EBCA87` | unsigned 64-bit value |
| XXH3-128 digest order | low 64 bits, then high 64 bits | each as big-endian `u64` |

The XXH3 call is `XXH3_128bits_withSeed(row_id, 0x9E3779B185EBCA87)`. The
digest bytes are `digest.low64.to_be_bytes()` followed by
`digest.high64.to_be_bytes()`.

## Identity bytes

Every identity has this layout:

```text
identity_version  u8       = 0x02
domain            u8
field_count       u32      big-endian
field             repeated field_count times
tuple_end         u8       = 0xFF
```

Each field starts with a type tag and a state tag:

```text
type_tag          u8
state_tag         u8       0x00 NULL, 0x01 VALUE
payload           type-specific, present only for VALUE
```

`field_count` must be at most `1024`. The decoder must find exactly that many
fields and then `tuple_end`. Any extra byte, missing field, unknown tag, or
wrong payload width is malformed input.

### Domain registry

Domain tags are stable. They are part of the identity, so equal field values in
different domains do not produce the same identity.

| Domain | Tag | Meaning |
|---|---:|---|
| `SCAN_KEY` | `0x01` | Eligible source identity-key fields |
| `KEYLESS_ROW` | `0x02` | All logical output fields |
| `GROUP_KEY` | `0x03` | `GROUP BY` fields |
| `JOIN_KEY` | `0x04` | Ordered child identities |
| `SET_KEY` | `0x05` | Set-operation identity fields |
| `WINDOW_KEY` | `0x06` | Partition and order identity fields |
| `SYNTHETIC` | `0x07` | Registered internal discriminator and fields |

No other domain tag is assigned. Pass-through operators retain the child
identity. Derived operators place child identities in explicitly framed fields.

## Privacy and operational handling

Opaque means “do not interpret as a key,” not “secret.” Canonical bytes can
contain reversible source values, especially for pass-through and keyless
identities. Keep `__pgt_row_id` out of default views, grants, logs, traces,
support bundles, and exported diagnostics unless an explicit owner-controlled
workflow requires it. Report identity length and a short diagnostic fingerprint
instead of complete bytes or reversible prefixes. External consumers must use
`BYTEA`; an old numeric identity cannot be cast into a V2 identity.

The v0.87.17 recreation preflight is read-only and does not return identity
bytes. Record external consumers and acknowledge their schema-change and
resnapshot plan before dropping V1 state. Writes during the recreation window
are not replayed.

### Type registry

Type tags identify semantic families, not PostgreSQL OIDs or typmods. Domains
use their base type tag. Runtime OIDs, typmods, enum OIDs, and catalog object
identifiers never enter row-ID bytes.

| Type family | Tag | Payload |
|---|---:|---|
| `bool` | `0x01` | one byte, `0x00` false or `0x01` true |
| `int2` | `0x02` | transformed signed 16-bit integer |
| `int4` | `0x03` | transformed signed 32-bit integer |
| `int8` | `0x04` | transformed signed 64-bit integer |
| `oid` | `0x05` | unsigned 32-bit integer |
| `float4` | `0x06` | transformed canonical IEEE-754 binary32 |
| `float8` | `0x07` | transformed canonical IEEE-754 binary64 |
| `numeric` | `0x08` | canonical ordered numeric payload |
| `text` | `0x09` | escaped database-encoding bytes |
| `varchar` | `0x0A` | escaped database-encoding bytes |
| `bpchar` | `0x0B` | trailing-space-normalized escaped bytes |
| `bytea` | `0x0C` | escaped raw bytes |
| `uuid` | `0x0D` | 16 bytes in network order |
| `date` | `0x0E` | transformed signed day count |
| `time` | `0x0F` | unsigned 64-bit microseconds |
| `timestamp` | `0x10` | transformed signed 64-bit microseconds |
| `timestamptz` | `0x11` | transformed UTC microseconds |
| `timetz` | `0x12` | transformed time and stored offset |
| `interval` | `0x13` | transformed signed 128-bit comparison value |
| `inet` | `0x14` | canonical family, prefix length, and address |
| `cidr` | `0x15` | canonical family, prefix length, and masked address |
| `macaddr` | `0x16` | six network-order bytes |
| `macaddr8` | `0x17` | eight network-order bytes |
| `bit` | `0x18` | bit length and escaped packed bits |
| `varbit` | `0x19` | bit length and escaped packed bits |
| enum | `0x1A` | escaped current label bytes |

Tags `0x1B` through `0x3F` are unassigned. Structural tags begin at `0x40` and
are not supported in v0.87.15. Unassigned tags must be rejected, not treated as
text.

## Scalar encoding rules

The `NULL` state has no payload. All NULL values for a field type use the same
bytes. SQL NULL is never a complete row identity.

For a signed integer of width `w`, encode `(value XOR (1 << (w - 1)))` as a
big-endian unsigned integer. This puts the smallest signed value first.

For `float4` and `float8`, map both signed zeros to positive zero. Map every NaN
to the positive quiet-NaN bit pattern, `0x7FC00000` or
`0x7FF8000000000000`. Then transform the bits as follows:

```text
if sign bit is set: transformed = bitwise_not(bits)
else:               transformed = bits XOR sign_bit
```

This orders negative infinity, finite values, positive infinity, and NaN.

For `numeric`, first parse the exact PostgreSQL numeric value. Normalize a
finite value by removing leading and trailing decimal zeroes and representing
it as `digits * 10^exponent`, where `digits` is the shortest non-zero decimal
string. Encode the payload as:

```text
class       u8       0x00 -Infinity, 0x01 negative finite,
                     0x02 zero, 0x03 positive finite,
                     0x04 +Infinity, 0x05 NaN
exponent    i32      sign-flipped, big-endian, for finite non-zero values
digit_count u32      big-endian
digits      bytes    ASCII '0' through '9'
```

For a negative finite value, bitwise-complement the encoded `exponent`,
`digit_count`, and `digits` bytes after the class byte. The sign class and this
complement make negative magnitudes sort in reverse. `1.0` and `1.00` therefore
encode identically.

`timestamp` and `timestamptz` use PostgreSQL's internal microsecond count.
`timetz` encodes the GMT-equivalent microseconds as transformed `i64`, followed
by the stored zone offset in seconds west of UTC as transformed `i32`.
`interval` uses the complete 128-bit `interval_cmp_value()` result, including
its 30-day month comparison rule, and applies the signed integer transform at
128 bits. It never truncates to 64 bits.

`inet` and `cidr` encode address family, prefix length, and address bytes.
`cidr` clears host bits before encoding. Enum values encode their current label,
not their OID. An enum label is at most 63 bytes in PostgreSQL 18.

## Escaping and framing

Variable payloads use unsigned-byte-order-preserving escaping. The byte `0x00`
is reserved for framing:

```text
payload 0x00 -> 0x00 0xFF
payload other bytes -> unchanged
value end  -> 0x00 0x00
```

The terminator sorts before a value byte after the same prefix. Fixed-width
payloads have the width implied by their type tag and have no terminator.

A nested tuple is encoded as a complete identity byte sequence without a new
identity version, followed by a `u32` big-endian byte length and the nested
bytes. Structural values must use a registered structural type tag. Composite
fields preserve field order and include their own field count and `0xFF` tuple
terminator. A nested tuple does not inherit the parent's domain.

Child identities in join, set-operation, and window keys use the same length
framing. This distinguishes `(A, BC)` from `(AB, C)`.

The complete identity is prefix-free because the field count and final
`0xFF` terminator are mandatory. The encoder rejects a payload that violates
its type's fixed width or framing rule.

## Resource limits

The limits apply to the complete identity and to every nested value:

| Limit | Value |
|---|---:|
| Maximum field count per tuple | `1024` |
| Maximum nesting depth | `32` |
| Maximum complete identity length | `1,048,576` bytes |
| Maximum nested tuple length | `1,048,576` bytes |
| Maximum enum label length | `63` bytes |

The encoder checks limits before allocating the result. Exceeding a limit is an
error with the field and type context. It never falls back to text or a hash.

## Probe V1

Probe V1 is an index accelerator. It is not an identity and is never stored as
a heap column. Let `P = 128`.

```text
if len(row_id) <= 128:
    probe = row_id
else:
    probe = row_id[0:128] || digest
```

For an overflow identity, `digest` is the 16-byte XXH3-128 result defined in
the version table. The probe is at most 144 bytes. The full row ID remains the
authoritative equality check, so a digest collision cannot merge, update, or
delete the wrong row. The probe index is never unique.

## Validation and support matrix

The encoder resolves the complete descriptor `(concrete type, typmod,
collation, equality operator family, nested metadata, PostgreSQL major)`. It
accepts only immutable or explicitly stable registry entries whose byte equality
matches PostgreSQL equality. Non-deterministic collations are rejected.

| PostgreSQL construct | v0.87.15 | Rule |
|---|---|---|
| Listed scalar families | Supported | Use the assigned tag and payload rule |
| Domains over supported scalars | Supported | Encode with the base type tag |
| Enums | Supported | Label encoding is stable; enum DDL invalidates dependent state |
| `C` and `POSIX` text collations | Supported | Native non-NULL B-tree order is preserved |
| Other deterministic text collations | Supported | Exact equality only; no native-order claim |
| Non-deterministic collations | Rejected | Equality is not a stable byte contract |
| Arrays | Rejected | No v0.87.15 structural tag |
| Ranges | Rejected | No v0.87.15 structural tag |
| Multiranges | Rejected | No v0.87.15 structural tag |
| `jsonb` | Rejected | No v0.87.15 structural tag |
| Composites and records | Rejected as identity fields | Nested framing is specified for later registered support |
| XML, geometric, money, and user-defined types | Rejected | No registered equality and byte contract |
| Non-default operator classes | Rejected | No registry proof of equality agreement |
| Partial or expression source indexes | Rejected | Not a complete row identity |
| Deferrable or non-immediate unique constraints | Rejected | Immediate maintenance cannot rely on them |
| Nullable `NULLS DISTINCT` unique keys | Rejected | Duplicate logical keys remain legal |
| Primary keys and eligible immediate unique keys | Supported | All key columns are `NOT NULL`, or the constraint is `NULLS NOT DISTINCT` |
| PostgreSQL majors other than 18 | Rejected | No verified contract |

The implementation must report the expression, resolved type, collation,
operator class, and rejected property. Uncertainty is rejection.

## Independent check vectors

The following vectors use `SCAN_KEY` (`0x01`) and one field. Hexadecimal output
is the complete identity.

| Input | Hexadecimal identity |
|---|---|
| `int4` value `1` | `02 01 00 00 00 01 03 01 80 00 00 01 FF` |
| `int4` NULL | `02 01 00 00 00 01 03 00 FF` |
| `text` value `a` | `02 01 00 00 00 01 09 01 61 00 00 FF` |
| `text` value containing `00` | `02 01 00 00 00 01 09 01 61 00 FF 62 00 00 FF` |

An independent implementation can verify the integer transform, NULL state,
variable-value terminator, zero-byte escape, field count, domain, and tuple
terminator from these vectors. Overflow probe vectors must compute XXH3-128
with the fixed seed and write low64 before high64 in big-endian order.

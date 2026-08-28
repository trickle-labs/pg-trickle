//! Versioned, typed row-identity V2 foundation.
//!
//! The complete identity is canonical bytes.  This module deliberately does
//! not use output functions, text formatting, or a hash as an identity.

use pgrx::datum::{Datum, FromDatum, IntoDatum, UnboxDatum};
use pgrx::heap_tuple::PgHeapTuple;
use pgrx::{AllocatedByRust, pg_extern, pg_sys};
use std::fmt::{self, Write};
use std::str::FromStr;

/// The immutable identity wire version.
pub const IDENTITY_VERSION_V2: u8 = 2;
/// The immutable bounded-probe wire version.
pub const PROBE_VERSION_V1: u8 = 1;
/// XXH3-128 seed used by [`row_probe_v1`].
pub const XXH3_128_SEED: u64 = 0x9E37_79B1_85EB_CA87;
/// Maximum identity prefix retained by the bounded probe.
pub const ROW_PROBE_PREFIX_BYTES: usize = 128;
/// Maximum nesting depth reserved for structural encoders.
pub const MAX_IDENTITY_NESTING_DEPTH: usize = 32;
/// Maximum number of fields in one tuple frame.
pub const MAX_TUPLE_FIELDS: usize = 1024;
/// Maximum complete identity accepted by the foundation.
pub const MAX_ENCODED_IDENTITY_BYTES: usize = 1024 * 1024;
/// PostgreSQL major supported by this registry.
pub const SUPPORTED_POSTGRES_MAJORS: &[u16] = &[18];

/// Field NULL marker.
pub const NULL_TAG: u8 = 0;
/// Field value marker.
pub const VALUE_TAG: u8 = 1;
/// Complete tuple marker.
pub const TUPLE_END_TAG: u8 = 0xff;
/// Escaped byte emitted for an input zero byte.
pub const VARIABLE_ESCAPE_TAG: [u8; 2] = [0x00, 0xff];
/// Terminator emitted after a variable-width payload.
pub const VARIABLE_END_TAG: [u8; 2] = [0x00, 0x00];

/// Stable identity-domain tags.
pub const DOMAIN_SCAN_KEY: u8 = 0x01;
pub const DOMAIN_KEYLESS_ROW: u8 = 0x02;
pub const DOMAIN_GROUP_KEY: u8 = 0x03;
pub const DOMAIN_JOIN_KEY: u8 = 0x04;
pub const DOMAIN_SET_KEY: u8 = 0x05;
pub const DOMAIN_WINDOW_KEY: u8 = 0x06;
pub const DOMAIN_SYNTHETIC: u8 = 0x07;

/// Stable scalar and structural type tags.
pub const TYPE_BOOL: u8 = 0x01;
pub const TYPE_INT2: u8 = 0x02;
pub const TYPE_INT4: u8 = 0x03;
pub const TYPE_INT8: u8 = 0x04;
pub const TYPE_OID: u8 = 0x05;
pub const TYPE_FLOAT4: u8 = 0x06;
pub const TYPE_FLOAT8: u8 = 0x07;
pub const TYPE_NUMERIC: u8 = 0x08;
pub const TYPE_TEXT: u8 = 0x09;
pub const TYPE_VARCHAR: u8 = 0x0a;
pub const TYPE_BPCHAR: u8 = 0x0b;
pub const TYPE_BYTEA: u8 = 0x0c;
pub const TYPE_UUID: u8 = 0x0d;
pub const TYPE_DATE: u8 = 0x0e;
pub const TYPE_TIME: u8 = 0x0f;
pub const TYPE_TIMESTAMP: u8 = 0x10;
pub const TYPE_TIMESTAMPTZ: u8 = 0x11;
pub const TYPE_TIMETZ: u8 = 0x12;
pub const TYPE_INTERVAL: u8 = 0x13;
pub const TYPE_INET: u8 = 0x14;
pub const TYPE_CIDR: u8 = 0x15;
pub const TYPE_MACADDR: u8 = 0x16;
pub const TYPE_MACADDR8: u8 = 0x17;
pub const TYPE_BIT: u8 = 0x18;
pub const TYPE_VARBIT: u8 = 0x19;
pub const TYPE_ENUM: u8 = 0x1a;
pub const TYPE_COMPOSITE: u8 = 0x40;

/// An identity domain has a stable wire tag and a distinct semantic meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityDomain {
    ScanKey,
    KeylessRow,
    GroupKey,
    JoinKey,
    SetKey,
    WindowKey,
    Synthetic,
}

impl IdentityDomain {
    pub const fn tag(self) -> u8 {
        match self {
            Self::ScanKey => DOMAIN_SCAN_KEY,
            Self::KeylessRow => DOMAIN_KEYLESS_ROW,
            Self::GroupKey => DOMAIN_GROUP_KEY,
            Self::JoinKey => DOMAIN_JOIN_KEY,
            Self::SetKey => DOMAIN_SET_KEY,
            Self::WindowKey => DOMAIN_WINDOW_KEY,
            Self::Synthetic => DOMAIN_SYNTHETIC,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::ScanKey => "SCAN_KEY",
            Self::KeylessRow => "KEYLESS_ROW",
            Self::GroupKey => "GROUP_KEY",
            Self::JoinKey => "JOIN_KEY",
            Self::SetKey => "SET_KEY",
            Self::WindowKey => "WINDOW_KEY",
            Self::Synthetic => "SYNTHETIC",
        }
    }
}

impl fmt::Display for IdentityDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for IdentityDomain {
    type Err = RowIdV2Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "SCAN_KEY" => Ok(Self::ScanKey),
            "KEYLESS_ROW" => Ok(Self::KeylessRow),
            "GROUP_KEY" => Ok(Self::GroupKey),
            "JOIN_KEY" => Ok(Self::JoinKey),
            "SET_KEY" => Ok(Self::SetKey),
            "WINDOW_KEY" => Ok(Self::WindowKey),
            "SYNTHETIC" => Ok(Self::Synthetic),
            _ => Err(RowIdV2Error::InvalidDomain(value.to_owned())),
        }
    }
}

/// Errors returned before the SQL boundary converts them to PostgreSQL errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RowIdV2Error {
    #[error("row identity v2: invalid identity domain '{0}'")]
    InvalidDomain(String),
    #[error("row identity v2: PostgreSQL major {0} is unsupported; supported majors: 18")]
    UnsupportedPostgresMajor(u16),
    #[error(
        "row identity v2: field '{field}' has unsupported structural type OID {oid} ({family})"
    )]
    UnsupportedStructuralType {
        field: String,
        oid: u32,
        family: &'static str,
    },
    #[error("row identity v2: field '{field}' has unknown or unsupported type OID {oid}")]
    UnknownType { field: String, oid: u32 },
    #[error("row identity v2: field '{field}' uses a non-deterministic collation")]
    NonDeterministicCollation { field: String },
    #[error("row identity v2: tuple has {found} fields; maximum is {maximum}")]
    TooManyFields { found: usize, maximum: usize },
    #[error("row identity v2: encoded identity exceeds {maximum} bytes")]
    EncodedIdentityTooLarge { maximum: usize },
    #[error("row identity v2: failed to read field '{field}': {detail}")]
    Datum { field: String, detail: String },
    #[error("row identity v2: field '{field}' has invalid metadata: {detail}")]
    InvalidFieldMetadata { field: String, detail: String },
    #[error("row identity v2: source key '{key}' is not eligible: {detail}")]
    InvalidSourceKey { key: String, detail: &'static str },
}

/// The scalar representation selected by a registry entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarSupport {
    Bool,
    SignedInteger { bytes: u8 },
    UnsignedInteger { bytes: u8 },
    Float { bytes: u8 },
    Numeric,
    Bytes,
    Text,
    Uuid,
    Date,
    Time,
    Timestamp,
    TimestampWithTimeZone,
    TimeWithTimeZone,
    Interval,
    Enum,
    Inet,
    Cidr,
    Macaddr { bytes: u8 },
    BitString,
}

#[derive(Debug, Clone, Copy)]
struct RawElement(pgrx::AnyElement);

impl FromDatum for RawElement {
    const GET_TYPOID: bool = true;

    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        typoid: pg_sys::Oid,
    ) -> Option<Self> {
        // SAFETY: callers provide the datum and its live PostgreSQL type OID.
        unsafe { pgrx::AnyElement::from_polymorphic_datum(datum, is_null, typoid) }.map(Self)
    }
}

impl IntoDatum for RawElement {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        Some(self.0.datum())
    }

    fn type_oid() -> pg_sys::Oid {
        pg_sys::ANYELEMENTOID
    }
}

// SAFETY: RawElement preserves the live PostgreSQL datum and its type metadata.
unsafe impl UnboxDatum for RawElement {
    type As<'src> = Self;

    unsafe fn unbox<'src>(datum: Datum<'src>) -> Self
    where
        Self: 'src,
    {
        // SAFETY: `get_by_index` does not call `unbox` for SQL NULL values.
        match unsafe {
            pgrx::AnyElement::from_polymorphic_datum(
                datum.sans_lifetime(),
                false,
                pg_sys::InvalidOid,
            )
        } {
            Some(value) => Self(value),
            None => {
                // SAFETY: the non-null datum contract above makes this branch unreachable.
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }
}

/// Volatility of the canonical encoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingVolatility {
    Immutable,
    Stable,
}

/// Static size bound published by a registry entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedSize {
    Fixed(usize),
    Unbounded,
}

/// One complete registry entry for a concrete PostgreSQL scalar type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypeDescriptor {
    pub oid: u32,
    pub name: &'static str,
    pub type_tag: u8,
    pub scalar: ScalarSupport,
    pub equality_canonical: bool,
    pub default_nonnull_btree_order_preserving: bool,
    pub volatility: EncodingVolatility,
    pub maximum_encoded_size: EncodedSize,
    pub supported_majors: &'static [u16],
}

impl TypeDescriptor {
    pub fn supports_major(self, major: u16) -> bool {
        self.supported_majors.contains(&major)
    }
}

const SCALAR_DESCRIPTORS: &[TypeDescriptor] = &[
    TypeDescriptor {
        oid: 16,
        name: "bool",
        type_tag: TYPE_BOOL,
        scalar: ScalarSupport::Bool,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(1),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 21,
        name: "int2",
        type_tag: TYPE_INT2,
        scalar: ScalarSupport::SignedInteger { bytes: 2 },
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(2),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 23,
        name: "int4",
        type_tag: TYPE_INT4,
        scalar: ScalarSupport::SignedInteger { bytes: 4 },
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(4),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 20,
        name: "int8",
        type_tag: TYPE_INT8,
        scalar: ScalarSupport::SignedInteger { bytes: 8 },
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(8),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 26,
        name: "oid",
        type_tag: TYPE_OID,
        scalar: ScalarSupport::UnsignedInteger { bytes: 4 },
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(4),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 700,
        name: "float4",
        type_tag: TYPE_FLOAT4,
        scalar: ScalarSupport::Float { bytes: 4 },
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(4),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 701,
        name: "float8",
        type_tag: TYPE_FLOAT8,
        scalar: ScalarSupport::Float { bytes: 8 },
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(8),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 17,
        name: "bytea",
        type_tag: TYPE_BYTEA,
        scalar: ScalarSupport::Bytes,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Unbounded,
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 25,
        name: "text",
        type_tag: TYPE_TEXT,
        scalar: ScalarSupport::Text,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: false,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Unbounded,
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1043,
        name: "varchar",
        type_tag: TYPE_VARCHAR,
        scalar: ScalarSupport::Text,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: false,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Unbounded,
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1042,
        name: "bpchar",
        type_tag: TYPE_BPCHAR,
        scalar: ScalarSupport::Text,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: false,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Unbounded,
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 2950,
        name: "uuid",
        type_tag: TYPE_UUID,
        scalar: ScalarSupport::Uuid,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(16),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1700,
        name: "numeric",
        type_tag: TYPE_NUMERIC,
        scalar: ScalarSupport::Numeric,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Unbounded,
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1082,
        name: "date",
        type_tag: TYPE_DATE,
        scalar: ScalarSupport::Date,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(4),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1083,
        name: "time",
        type_tag: TYPE_TIME,
        scalar: ScalarSupport::Time,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(8),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1114,
        name: "timestamp",
        type_tag: TYPE_TIMESTAMP,
        scalar: ScalarSupport::Timestamp,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(8),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1184,
        name: "timestamptz",
        type_tag: TYPE_TIMESTAMPTZ,
        scalar: ScalarSupport::TimestampWithTimeZone,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(8),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1266,
        name: "timetz",
        type_tag: TYPE_TIMETZ,
        scalar: ScalarSupport::TimeWithTimeZone,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(12),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1186,
        name: "interval",
        type_tag: TYPE_INTERVAL,
        scalar: ScalarSupport::Interval,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(16),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 869,
        name: "inet",
        type_tag: TYPE_INET,
        scalar: ScalarSupport::Inet,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(18),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 650,
        name: "cidr",
        type_tag: TYPE_CIDR,
        scalar: ScalarSupport::Cidr,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(18),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 829,
        name: "macaddr",
        type_tag: TYPE_MACADDR,
        scalar: ScalarSupport::Macaddr { bytes: 6 },
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(6),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 774,
        name: "macaddr8",
        type_tag: TYPE_MACADDR8,
        scalar: ScalarSupport::Macaddr { bytes: 8 },
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Fixed(8),
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1560,
        name: "bit",
        type_tag: TYPE_BIT,
        scalar: ScalarSupport::BitString,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Unbounded,
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
    TypeDescriptor {
        oid: 1562,
        name: "varbit",
        type_tag: TYPE_VARBIT,
        scalar: ScalarSupport::BitString,
        equality_canonical: true,
        default_nonnull_btree_order_preserving: true,
        volatility: EncodingVolatility::Immutable,
        maximum_encoded_size: EncodedSize::Unbounded,
        supported_majors: SUPPORTED_POSTGRES_MAJORS,
    },
];

const ENUM_DESCRIPTOR: TypeDescriptor = TypeDescriptor {
    oid: 0,
    name: "enum",
    type_tag: TYPE_ENUM,
    scalar: ScalarSupport::Enum,
    equality_canonical: true,
    default_nonnull_btree_order_preserving: false,
    volatility: EncodingVolatility::Stable,
    maximum_encoded_size: EncodedSize::Fixed(65),
    supported_majors: SUPPORTED_POSTGRES_MAJORS,
};

/// Explicit registry for the supported V2 foundation types.
#[derive(Debug, Clone, Copy, Default)]
pub struct TypeRegistry;

impl TypeRegistry {
    pub const fn new() -> Self {
        Self
    }

    pub fn supports_major(&self, major: u16) -> bool {
        SUPPORTED_POSTGRES_MAJORS.contains(&major)
    }

    pub fn descriptor(&self, oid: u32) -> Result<&'static TypeDescriptor, RowIdV2Error> {
        if let Some(descriptor) = SCALAR_DESCRIPTORS
            .iter()
            .find(|descriptor| descriptor.oid == oid)
        {
            return Ok(descriptor);
        }
        if let Some(family) = structural_family(oid) {
            return Err(RowIdV2Error::UnsupportedStructuralType {
                field: "<unknown>".to_owned(),
                oid,
                family,
            });
        }
        Err(RowIdV2Error::UnknownType {
            field: "<unknown>".to_owned(),
            oid,
        })
    }

    pub fn validate_type(
        &self,
        field: &str,
        oid: u32,
        postgres_major: u16,
    ) -> Result<&'static TypeDescriptor, RowIdV2Error> {
        if !self.supports_major(postgres_major) {
            return Err(RowIdV2Error::UnsupportedPostgresMajor(postgres_major));
        }
        match self.descriptor(oid) {
            Ok(descriptor) => Ok(descriptor),
            Err(RowIdV2Error::UnsupportedStructuralType { oid, family, .. }) => {
                Err(RowIdV2Error::UnsupportedStructuralType {
                    field: field.to_owned(),
                    oid,
                    family,
                })
            }
            Err(RowIdV2Error::UnknownType { oid, .. }) => Err(RowIdV2Error::UnknownType {
                field: field.to_owned(),
                oid,
            }),
            Err(error) => Err(error),
        }
    }
}

fn structural_family(oid: u32) -> Option<&'static str> {
    match oid {
        114 | 3802 => Some("JSON/JSONB"),
        2249 | 2287 => Some("composite/record"),
        2277 | 2776 | 5078 | 5079 => Some("array polymorphic"),
        3904 | 3906 | 3908 | 3910 | 3912 | 3926 => Some("range"),
        4451 | 4532 | 4533 | 4534 | 4535 | 4536 => Some("multirange"),
        1000..=1022
        | 1027..=1028
        | 1034
        | 1040..=1041
        | 1049
        | 1115
        | 1182..=1187
        | 1231
        | 1270
        | 1561
        | 1563
        | 2201
        | 2207..=2211
        | 4192 => Some("array"),
        _ => None,
    }
}

/// A field passed to the pure tuple framer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodedField<'a> {
    pub type_tag: u8,
    pub payload: Option<&'a [u8]>,
}

impl<'a> EncodedField<'a> {
    pub const fn null(type_tag: u8) -> Self {
        Self {
            type_tag,
            payload: None,
        }
    }

    pub const fn value(type_tag: u8, payload: &'a [u8]) -> Self {
        Self {
            type_tag,
            payload: Some(payload),
        }
    }
}

/// Encode one tuple with explicit field, NULL, value, and end framing.
pub fn encode_tuple(
    domain: IdentityDomain,
    fields: &[EncodedField<'_>],
) -> Result<Vec<u8>, RowIdV2Error> {
    if fields.len() > MAX_TUPLE_FIELDS {
        return Err(RowIdV2Error::TooManyFields {
            found: fields.len(),
            maximum: MAX_TUPLE_FIELDS,
        });
    }
    let payload_bytes = fields.iter().try_fold(0usize, |size, field| {
        size.checked_add(field.payload.map_or(0, <[u8]>::len))
    });
    let output_bytes = match payload_bytes
        .and_then(|payload_bytes| 6usize.checked_add(fields.len() * 2 + payload_bytes + 1))
    {
        Some(size) if size <= MAX_ENCODED_IDENTITY_BYTES => size,
        _ => {
            return Err(RowIdV2Error::EncodedIdentityTooLarge {
                maximum: MAX_ENCODED_IDENTITY_BYTES,
            });
        }
    };
    let mut output = Vec::with_capacity(output_bytes);
    output.push(IDENTITY_VERSION_V2);
    output.push(domain.tag());
    output.extend_from_slice(&(fields.len() as u32).to_be_bytes());
    for field in fields {
        if !is_known_type_tag(field.type_tag) {
            return Err(RowIdV2Error::InvalidFieldMetadata {
                field: "<framing>".to_owned(),
                detail: format!("type tag {:#x} is unsupported", field.type_tag),
            });
        }
        if let Some(width) = fixed_width(field.type_tag)
            && field.payload.is_some_and(|payload| payload.len() != width)
        {
            return Err(RowIdV2Error::InvalidFieldMetadata {
                field: "<framing>".to_owned(),
                detail: format!(
                    "type tag {:#x} requires a {width}-byte payload",
                    field.type_tag
                ),
            });
        }
        output.push(field.type_tag);
        match field.payload {
            None => output.push(NULL_TAG),
            Some(payload) => {
                output.push(VALUE_TAG);
                output.extend_from_slice(payload);
            }
        }
        if output.len() > MAX_ENCODED_IDENTITY_BYTES {
            return Err(RowIdV2Error::EncodedIdentityTooLarge {
                maximum: MAX_ENCODED_IDENTITY_BYTES,
            });
        }
    }
    if output.len() >= MAX_ENCODED_IDENTITY_BYTES {
        return Err(RowIdV2Error::EncodedIdentityTooLarge {
            maximum: MAX_ENCODED_IDENTITY_BYTES,
        });
    }
    output.push(TUPLE_END_TAG);
    Ok(output)
}

fn wire_error(detail: impl Into<String>) -> RowIdV2Error {
    RowIdV2Error::InvalidFieldMetadata {
        field: "<wire>".to_owned(),
        detail: detail.into(),
    }
}

fn variable_end(input: &[u8], mut offset: usize) -> Result<(usize, usize), RowIdV2Error> {
    let mut decoded_bytes = 0;
    while offset < input.len() {
        if input[offset] != 0 {
            offset += 1;
            decoded_bytes += 1;
            continue;
        }
        let tag = input
            .get(offset + 1)
            .ok_or_else(|| wire_error("unterminated variable payload"))?;
        match *tag {
            0xff => {
                offset += 2;
                decoded_bytes += 1;
            }
            0x00 => return Ok((offset + 2, decoded_bytes)),
            _ => return Err(wire_error("invalid variable payload escape")),
        }
    }
    Err(wire_error("unterminated variable payload"))
}

fn fixed_width(type_tag: u8) -> Option<usize> {
    match type_tag {
        TYPE_BOOL => Some(1),
        TYPE_INT2 => Some(2),
        TYPE_INT4 => Some(4),
        TYPE_INT8 => Some(8),
        TYPE_OID => Some(4),
        TYPE_FLOAT4 => Some(4),
        TYPE_FLOAT8 => Some(8),
        TYPE_UUID => Some(16),
        TYPE_DATE => Some(4),
        TYPE_TIME => Some(8),
        TYPE_TIMESTAMP | TYPE_TIMESTAMPTZ => Some(8),
        TYPE_TIMETZ => Some(12),
        TYPE_INTERVAL => Some(16),
        TYPE_MACADDR => Some(6),
        TYPE_MACADDR8 => Some(8),
        _ => None,
    }
}

fn is_variable_type(type_tag: u8) -> bool {
    matches!(
        type_tag,
        TYPE_TEXT | TYPE_VARCHAR | TYPE_BPCHAR | TYPE_BYTEA | TYPE_ENUM
    )
}

fn is_network_type(type_tag: u8) -> bool {
    matches!(type_tag, TYPE_INET | TYPE_CIDR)
}

fn is_bit_type(type_tag: u8) -> bool {
    matches!(type_tag, TYPE_BIT | TYPE_VARBIT)
}

fn is_known_type_tag(type_tag: u8) -> bool {
    fixed_width(type_tag).is_some()
        || type_tag == TYPE_NUMERIC
        || is_variable_type(type_tag)
        || is_network_type(type_tag)
        || is_bit_type(type_tag)
}

/// Validate exact V2 framing before a complete identity is accepted.
pub fn validate_identity_v2(input: &[u8]) -> Result<(), RowIdV2Error> {
    if input.len() > MAX_ENCODED_IDENTITY_BYTES {
        return Err(RowIdV2Error::EncodedIdentityTooLarge {
            maximum: MAX_ENCODED_IDENTITY_BYTES,
        });
    }
    if input.len() < 7 {
        return Err(wire_error("identity is shorter than its framing"));
    }
    if input[0] != IDENTITY_VERSION_V2 {
        return Err(wire_error(format!(
            "unsupported identity version {}",
            input[0]
        )));
    }
    if IdentityDomain::from_str(match input[1] {
        DOMAIN_SCAN_KEY => "SCAN_KEY",
        DOMAIN_KEYLESS_ROW => "KEYLESS_ROW",
        DOMAIN_GROUP_KEY => "GROUP_KEY",
        DOMAIN_JOIN_KEY => "JOIN_KEY",
        DOMAIN_SET_KEY => "SET_KEY",
        DOMAIN_WINDOW_KEY => "WINDOW_KEY",
        DOMAIN_SYNTHETIC => "SYNTHETIC",
        _ => {
            return Err(wire_error(format!(
                "unknown identity domain tag {}",
                input[1]
            )));
        }
    })
    .is_err()
    {
        return Err(wire_error("unknown identity domain"));
    }
    let field_count = u32::from_be_bytes(
        input[2..6]
            .try_into()
            .map_err(|_| wire_error("field count is malformed"))?,
    ) as usize;
    if field_count > MAX_TUPLE_FIELDS {
        return Err(RowIdV2Error::TooManyFields {
            found: field_count,
            maximum: MAX_TUPLE_FIELDS,
        });
    }
    let mut offset = 6;
    for _ in 0..field_count {
        let type_tag = *input
            .get(offset)
            .ok_or_else(|| wire_error("missing field type tag"))?;
        offset += 1;
        if type_tag == 0
            || (!fixed_width(type_tag).is_some()
                && !is_variable_type(type_tag)
                && !is_network_type(type_tag)
                && !is_bit_type(type_tag)
                && type_tag != TYPE_NUMERIC)
        {
            return Err(wire_error(format!("unknown field type tag {type_tag:#x}")));
        }
        let state = *input
            .get(offset)
            .ok_or_else(|| wire_error("missing field state tag"))?;
        offset += 1;
        match state {
            NULL_TAG => {}
            VALUE_TAG => {
                if let Some(width) = fixed_width(type_tag) {
                    let start = offset;
                    let end = offset
                        .checked_add(width)
                        .filter(|offset| *offset <= input.len())
                        .ok_or_else(|| wire_error("fixed-width field payload is truncated"))?;
                    if type_tag == TYPE_BOOL && input[start] > 1 {
                        return Err(wire_error("boolean payload is not 0 or 1"));
                    }
                    offset = end;
                } else if type_tag == TYPE_NUMERIC {
                    let class = *input
                        .get(offset)
                        .ok_or_else(|| wire_error("numeric class is missing"))?;
                    offset += 1;
                    if matches!(class, 0x01 | 0x03) {
                        let end = offset
                            .checked_add(8)
                            .filter(|end| *end <= input.len())
                            .ok_or_else(|| wire_error("numeric header is truncated"))?;
                        let mut header = [0u8; 8];
                        header.copy_from_slice(&input[offset..end]);
                        if class == 0x01 {
                            header.iter_mut().for_each(|byte| *byte = !*byte);
                        }
                        let count = u32::from_be_bytes(
                            header[4..]
                                .try_into()
                                .map_err(|_| wire_error("numeric digit count is malformed"))?,
                        ) as usize;
                        if count == 0 {
                            return Err(wire_error("finite numeric has no digits"));
                        }
                        let end = end
                            .checked_add(count)
                            .filter(|end| *end <= input.len())
                            .ok_or_else(|| wire_error("numeric digits are truncated"))?;
                        let digits = &input[end - count..end];
                        if !digits.iter().enumerate().all(|(index, byte)| {
                            let byte = if class == 0x01 { !*byte } else { *byte };
                            byte.is_ascii_digit()
                                && (index > 0 || byte != b'0')
                                && (index + 1 < digits.len() || byte != b'0')
                        }) {
                            return Err(wire_error("numeric digits are not canonical ASCII"));
                        }
                        offset = end;
                    } else if !matches!(class, 0x00 | 0x02 | 0x04 | 0x05) {
                        return Err(wire_error("unknown numeric class"));
                    }
                } else if is_network_type(type_tag) {
                    let family = *input
                        .get(offset)
                        .ok_or_else(|| wire_error("network family is missing"))?;
                    let width = match family {
                        2 => 4,
                        3 => 16,
                        _ => return Err(wire_error("unknown network address family")),
                    };
                    let prefix = *input
                        .get(offset + 1)
                        .ok_or_else(|| wire_error("network prefix is missing"))?;
                    if prefix as usize > width * 8 {
                        return Err(wire_error("network prefix is outside address width"));
                    }
                    let end = offset
                        .checked_add(width + 2)
                        .filter(|offset| *offset <= input.len())
                        .ok_or_else(|| wire_error("network payload is truncated"))?;
                    if type_tag == TYPE_CIDR {
                        let address = &input[offset + 2..end];
                        let whole_bytes = prefix as usize / 8;
                        let remaining_bits = prefix % 8;
                        if (remaining_bits != 0
                            && address[whole_bytes] & (0xff >> remaining_bits) != 0)
                            || address[whole_bytes + usize::from(remaining_bits != 0)..]
                                .iter()
                                .any(|byte| *byte != 0)
                        {
                            return Err(wire_error("cidr host bits are not canonical"));
                        }
                    }
                    offset = end;
                } else if is_bit_type(type_tag) {
                    let bit_length = u32::from_be_bytes(
                        input
                            .get(offset..offset + 4)
                            .ok_or_else(|| wire_error("bit length is truncated"))?
                            .try_into()
                            .map_err(|_| wire_error("bit length is malformed"))?,
                    );
                    let (end, decoded_bytes) = variable_end(input, offset + 4)?;
                    if decoded_bytes != bit_length.div_ceil(8) as usize {
                        return Err(wire_error("bit payload has the wrong width"));
                    }
                    offset = end;
                } else if is_variable_type(type_tag) {
                    let (end, decoded_bytes) = variable_end(input, offset)?;
                    let _ = decoded_bytes;
                    offset = end;
                }
            }
            _ => return Err(wire_error(format!("unknown field state tag {state:#x}"))),
        }
    }
    if input.get(offset) != Some(&TUPLE_END_TAG) || offset + 1 != input.len() {
        return Err(wire_error("identity has missing or trailing tuple framing"));
    }
    Ok(())
}

/// Encode a boolean payload.
pub const fn encode_bool(value: bool) -> [u8; 1] {
    [value as u8]
}

/// Encode a signed integer with its sign bit flipped for unsigned lexicographic order.
pub fn encode_signed(value: i128, bytes: usize) -> Result<Vec<u8>, RowIdV2Error> {
    if !(1..=16).contains(&bytes) {
        return Err(RowIdV2Error::InvalidFieldMetadata {
            field: "<integer>".to_owned(),
            detail: format!("signed width {bytes} is outside 1..=16"),
        });
    }
    if bytes < 16 {
        let minimum = -(1i128 << (bytes * 8 - 1));
        let maximum = (1i128 << (bytes * 8 - 1)) - 1;
        if !(minimum..=maximum).contains(&value) {
            return Err(RowIdV2Error::InvalidFieldMetadata {
                field: "<integer>".to_owned(),
                detail: format!("signed value does not fit in {bytes} bytes"),
            });
        }
    }
    let transformed = value ^ (1i128 << (bytes * 8 - 1));
    Ok(transformed.to_be_bytes()[16 - bytes..].to_vec())
}

/// Encode an unsigned integer in big-endian order.
pub fn encode_unsigned(value: u128, bytes: usize) -> Result<Vec<u8>, RowIdV2Error> {
    if !(1..=16).contains(&bytes) || (bytes < 16 && value >= (1u128 << (bytes * 8))) {
        return Err(RowIdV2Error::InvalidFieldMetadata {
            field: "<integer>".to_owned(),
            detail: format!("unsigned value does not fit in {bytes} bytes"),
        });
    }
    Ok(value.to_be_bytes()[16 - bytes..].to_vec())
}

/// Encode a signed 64-bit integer.
pub fn encode_signed_i64(value: i64) -> [u8; 8] {
    (value ^ i64::MIN).to_be_bytes()
}

/// Encode an unsigned 64-bit integer.
pub const fn encode_unsigned_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Return the canonical IEEE-754 bits used before the sortable transform.
pub const fn canonical_float32_bits(value: f32) -> u32 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        0x7fc0_0000
    } else {
        value.to_bits()
    }
}

/// Return the canonical IEEE-754 bits used before the sortable transform.
pub const fn canonical_float64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        value.to_bits()
    }
}

/// Encode a float32 using the standard sortable IEEE transform.
pub fn encode_float32(value: f32) -> [u8; 4] {
    let bits = canonical_float32_bits(value);
    let sortable = if bits & 0x8000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000
    };
    sortable.to_be_bytes()
}

/// Encode a float64 using the standard sortable IEEE transform.
pub fn encode_float64(value: f64) -> [u8; 8] {
    let bits = canonical_float64_bits(value);
    let sortable = if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    };
    sortable.to_be_bytes()
}

/// Escape arbitrary bytes and append the variable-value terminator.
pub fn encode_bytes(value: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(value.len() + 2);
    for byte in value {
        if *byte == 0 {
            output.extend_from_slice(&VARIABLE_ESCAPE_TAG);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&VARIABLE_END_TAG);
    output
}

/// Text uses the database-encoding bytes, with the same canonical escaping as bytea.
pub fn encode_text(value: &[u8]) -> Vec<u8> {
    encode_bytes(value)
}

/// Encode `bpchar` after applying PostgreSQL's trailing-space equality rule.
pub fn encode_bpchar(value: &[u8]) -> Vec<u8> {
    encode_bytes(value.trim_ascii_end())
}

fn encode_numeric_datum(value: &RawElement) -> Result<Vec<u8>, RowIdV2Error> {
    let payload = raw_varlena_payload(value);
    if payload.len() < 2 {
        return Err(RowIdV2Error::InvalidFieldMetadata {
            field: "<numeric>".to_owned(),
            detail: "numeric datum is shorter than its header".to_owned(),
        });
    }
    let header = u16::from_ne_bytes([payload[0], payload[1]]);
    let flags = header & 0xc000;
    if flags == 0xc000 {
        return match header & 0xf000 {
            0xc000 => Ok(vec![0x05]),
            0xd000 => Ok(vec![0x04]),
            0xf000 => Ok(vec![0x00]),
            _ => Err(RowIdV2Error::InvalidFieldMetadata {
                field: "<numeric>".to_owned(),
                detail: "numeric special value has an invalid header".to_owned(),
            }),
        };
    }
    let (negative, weight, digit_bytes) = if flags == 0x8000 {
        let weight = if header & 0x0040 != 0 {
            ((header & 0x003f) | 0xffc0) as i16
        } else {
            (header & 0x003f) as i16
        };
        (header & 0x2000 != 0, weight, &payload[2..])
    } else if flags == 0x0000 || flags == 0x4000 {
        if payload.len() < 4 {
            return Err(RowIdV2Error::InvalidFieldMetadata {
                field: "<numeric>".to_owned(),
                detail: "numeric long datum is shorter than its header".to_owned(),
            });
        }
        (
            flags == 0x4000,
            i16::from_ne_bytes([payload[2], payload[3]]),
            &payload[4..],
        )
    } else {
        return Err(RowIdV2Error::InvalidFieldMetadata {
            field: "<numeric>".to_owned(),
            detail: "numeric datum has an invalid sign format".to_owned(),
        });
    };
    let (digit_words, remainder) = digit_bytes.as_chunks::<2>();
    if !remainder.is_empty() {
        return Err(RowIdV2Error::InvalidFieldMetadata {
            field: "<numeric>".to_owned(),
            detail: "numeric digit payload is truncated".to_owned(),
        });
    }
    let mut digits = Vec::with_capacity(digit_words.len());
    for word in digit_words {
        let digit = u16::from_ne_bytes(*word);
        if digit >= 10_000 {
            return Err(RowIdV2Error::InvalidFieldMetadata {
                field: "<numeric>".to_owned(),
                detail: "numeric base-10000 digit is out of range".to_owned(),
            });
        }
        digits.push(digit);
    }
    let leading_zeroes = digits.iter().position(|digit| *digit != 0);
    let Some(leading_zeroes) = leading_zeroes else {
        return Ok(vec![0x02]);
    };
    let weight = weight.checked_sub(leading_zeroes as i16).ok_or_else(|| {
        RowIdV2Error::InvalidFieldMetadata {
            field: "<numeric>".to_owned(),
            detail: "numeric weight is outside the supported range".to_owned(),
        }
    })?;
    digits.drain(..leading_zeroes);
    while digits.last() == Some(&0) {
        digits.pop();
    }
    let first_group_digits = digits[0].to_string().len() as i64;
    let mut decimal = digits[0].to_string();
    for digit in &digits[1..] {
        write!(&mut decimal, "{digit:04}").map_err(|_| RowIdV2Error::InvalidFieldMetadata {
            field: "<numeric>".to_owned(),
            detail: "numeric digit formatting failed".to_owned(),
        })?;
    }
    let trailing_zeroes = decimal
        .as_bytes()
        .iter()
        .rev()
        .take_while(|byte| **byte == b'0')
        .count();
    decimal.truncate(decimal.len() - trailing_zeroes);
    let exponent = i64::from(weight) * 4 + first_group_digits - 1;
    let exponent = i32::try_from(exponent).map_err(|_| RowIdV2Error::InvalidFieldMetadata {
        field: "<numeric>".to_owned(),
        detail: "numeric exponent is outside the supported range".to_owned(),
    })?;
    let mut body = Vec::with_capacity(8 + decimal.len());
    body.extend_from_slice(&encode_signed_i32(exponent).to_be_bytes());
    body.extend_from_slice(&(decimal.len() as u32).to_be_bytes());
    body.extend_from_slice(decimal.as_bytes());
    if negative {
        body.iter_mut().for_each(|byte| *byte = !*byte);
    }
    let mut output = Vec::with_capacity(body.len() + 1);
    output.push(if negative { 0x01 } else { 0x03 });
    output.extend_from_slice(&body);
    Ok(output)
}

/// Encode PostgreSQL's normalized numeric text without using `numeric_out`.
pub fn encode_numeric_text(value: &str) -> Result<Vec<u8>, RowIdV2Error> {
    let (class, body) = match value {
        "-Infinity" => return Ok(vec![0x00]),
        "Infinity" => return Ok(vec![0x04]),
        "NaN" => return Ok(vec![0x05]),
        _ => {
            let (negative, value) = if let Some(value) = value.strip_prefix('-') {
                (true, value)
            } else {
                (false, value.strip_prefix('+').unwrap_or(value))
            };
            let (mantissa, exponent) = value
                .find('e')
                .or_else(|| value.find('E'))
                .map_or(Ok((value, 0)), |index| {
                    value[index + 1..]
                        .parse::<i64>()
                        .map(|exponent| (&value[..index], exponent))
                })
                .map_err(|_| RowIdV2Error::InvalidFieldMetadata {
                    field: "<numeric>".to_owned(),
                    detail: "numeric exponent is invalid".to_owned(),
                })?;
            let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
            if integer.is_empty()
                || !integer.bytes().all(|byte| byte.is_ascii_digit())
                || !fraction.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(RowIdV2Error::InvalidFieldMetadata {
                    field: "<numeric>".to_owned(),
                    detail: "numeric value is not a decimal".to_owned(),
                });
            }
            let mut digits = integer.as_bytes().to_vec();
            digits.extend_from_slice(fraction.as_bytes());
            let leading_zeroes = digits
                .iter()
                .position(|byte| *byte != b'0')
                .unwrap_or(digits.len());
            if leading_zeroes == digits.len() {
                return Ok(vec![0x02]);
            }
            digits.drain(..leading_zeroes);
            let trailing_zeroes = digits
                .iter()
                .rev()
                .take_while(|byte| **byte == b'0')
                .count();
            digits.truncate(digits.len() - trailing_zeroes);
            let exponent = integer.len() as i64 - leading_zeroes as i64 - 1 + exponent;
            let exponent =
                i32::try_from(exponent).map_err(|_| RowIdV2Error::InvalidFieldMetadata {
                    field: "<numeric>".to_owned(),
                    detail: "numeric exponent is outside the supported range".to_owned(),
                })?;
            let mut body = Vec::with_capacity(8 + digits.len());
            body.extend_from_slice(&(encode_signed_i32(exponent)).to_be_bytes());
            body.extend_from_slice(&(digits.len() as u32).to_be_bytes());
            body.extend_from_slice(&digits);
            (if negative { 0x01 } else { 0x03 }, body)
        }
    };
    if class == 0x01 {
        let mut body = body;
        body.iter_mut().for_each(|byte| *byte = !*byte);
        let mut output = vec![class];
        output.extend_from_slice(&body);
        Ok(output)
    } else {
        let mut output = vec![class];
        output.extend_from_slice(&body);
        Ok(output)
    }
}

fn encode_signed_i32(value: i32) -> u32 {
    (value ^ i32::MIN) as u32
}

/// Encode PostgreSQL's interval comparison value using its complete 128-bit range.
pub fn encode_interval_value(months: i32, days: i32, micros: i64) -> [u8; 16] {
    let comparison_value = (months as i128 * pgrx::pg_sys::DAYS_PER_MONTH as i128 + days as i128)
        * 86_400_000_000i128
        + micros as i128;
    let transformed = comparison_value ^ (1i128 << 127);
    transformed.to_be_bytes()
}

/// Encode a `timetz` value as its UTC time followed by its stored offset.
pub fn encode_timetz_value(time: i64, zone_seconds_west: i32) -> [u8; 12] {
    let utc_time = time + zone_seconds_west as i64 * 1_000_000;
    let mut output = [0u8; 12];
    output[..8].copy_from_slice(&encode_signed_i64(utc_time));
    output[8..].copy_from_slice(&encode_signed_i32(zone_seconds_west).to_be_bytes());
    output
}

/// Encode the payload bytes of an `inet` or `cidr` datum.
pub fn encode_network_payload(payload: &[u8], cidr: bool) -> Result<Vec<u8>, RowIdV2Error> {
    if payload.len() < 2 {
        return Err(RowIdV2Error::InvalidFieldMetadata {
            field: "<network>".to_owned(),
            detail: "network payload is shorter than its header".to_owned(),
        });
    }
    let (family, width) = match payload[0] {
        2 => (2usize, 4usize),
        3 => (3usize, 16usize),
        _ => {
            return Err(RowIdV2Error::InvalidFieldMetadata {
                field: "<network>".to_owned(),
                detail: format!("unknown network address family {}", payload[0]),
            });
        }
    };
    let prefix = payload[1];
    if prefix as usize > width * 8 || payload.len() != width + 2 {
        return Err(RowIdV2Error::InvalidFieldMetadata {
            field: "<network>".to_owned(),
            detail: "network payload has an invalid prefix or address width".to_owned(),
        });
    }
    let mut address = payload[2..].to_vec();
    if cidr && !prefix.is_multiple_of(8) {
        let index = prefix as usize / 8;
        address[index] &= 0xff << (8 - prefix % 8);
        address[index + 1..].fill(0);
    } else if cidr {
        address[prefix as usize / 8..].fill(0);
    }
    let mut output = Vec::with_capacity(width + 2);
    output.extend_from_slice(&[family as u8, prefix]);
    output.extend_from_slice(&address);
    Ok(output)
}

/// Encode a PostgreSQL `bit` or `varbit` varlena payload.
pub fn encode_bit_payload(payload: &[u8]) -> Result<Vec<u8>, RowIdV2Error> {
    if payload.len() < 4 {
        return Err(RowIdV2Error::InvalidFieldMetadata {
            field: "<bitstring>".to_owned(),
            detail: "bitstring payload is shorter than its bit length".to_owned(),
        });
    }
    let bit_length = u32::from_ne_bytes(payload[..4].try_into().map_err(|_| {
        RowIdV2Error::InvalidFieldMetadata {
            field: "<bitstring>".to_owned(),
            detail: "bit length header is malformed".to_owned(),
        }
    })?);
    let byte_length = bit_length.div_ceil(8) as usize;
    if payload.len() < 4 + byte_length {
        return Err(RowIdV2Error::InvalidFieldMetadata {
            field: "<bitstring>".to_owned(),
            detail: "bitstring payload is truncated".to_owned(),
        });
    }
    let mut bits = payload[4..4 + byte_length].to_vec();
    if let Some(last) = bits.last_mut() {
        let used = bit_length % 8;
        if used != 0 {
            *last &= 0xff << (8 - used);
        }
    }
    let mut output = Vec::with_capacity(4 + bits.len() + 2);
    output.extend_from_slice(&bit_length.to_be_bytes());
    output.extend_from_slice(&encode_bytes(&bits));
    Ok(output)
}

/// UUID payloads are already 16 network-order bytes.
pub const fn encode_uuid(value: &[u8; 16]) -> [u8; 16] {
    *value
}

/// Validate a pure identity schema before any state is created.
pub fn validate_identity_schema(
    domain: &str,
    field_type_oids: &[u32],
) -> Result<IdentityDomain, RowIdV2Error> {
    let domain = IdentityDomain::from_str(domain)?;
    if field_type_oids.len() > MAX_TUPLE_FIELDS {
        return Err(RowIdV2Error::TooManyFields {
            found: field_type_oids.len(),
            maximum: MAX_TUPLE_FIELDS,
        });
    }
    let registry = TypeRegistry::new();
    for (index, oid) in field_type_oids.iter().copied().enumerate() {
        registry.validate_type(&format!("field {index}"), oid, 18)?;
    }
    Ok(domain)
}

/// Validate a collation contract independently of PostgreSQL catalog access.
pub fn validate_collation(field: &str, deterministic: bool) -> Result<(), RowIdV2Error> {
    if deterministic {
        Ok(())
    } else {
        Err(RowIdV2Error::NonDeterministicCollation {
            field: field.to_owned(),
        })
    }
}

/// Catalog facts needed to validate a source identity key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceKeyContract {
    pub primary: bool,
    pub columns_not_null: bool,
    pub nulls_not_distinct: bool,
    pub partial: bool,
    pub expression: bool,
    pub deferrable: bool,
    pub immediate: bool,
    pub default_btree_operator_class: bool,
}

/// Apply the source-key eligibility policy without holding an SPI connection.
pub fn validate_source_key(key: &str, contract: SourceKeyContract) -> Result<(), RowIdV2Error> {
    for (rejected, detail) in [
        (
            contract.primary && !contract.columns_not_null && !contract.nulls_not_distinct,
            "primary keys must identify non-NULL values",
        ),
        (
            contract.partial,
            "partial indexes are not complete row identities",
        ),
        (
            contract.expression,
            "expression indexes do not identify source columns",
        ),
        (
            contract.deferrable,
            "deferrable keys are not safe for immediate maintenance",
        ),
        (!contract.immediate, "the key constraint is not immediate"),
        (
            !contract.default_btree_operator_class,
            "the operator class has no verified equality contract",
        ),
        (
            !contract.columns_not_null && !contract.nulls_not_distinct,
            "nullable NULLS DISTINCT keys can contain duplicate logical keys",
        ),
    ] {
        if rejected {
            return Err(RowIdV2Error::InvalidSourceKey {
                key: key.to_owned(),
                detail,
            });
        }
    }
    if contract.primary {
        return Ok(());
    }
    Ok(())
}

/// Return the full identity for short inputs, or a bounded prefix plus XXH3-128 digest.
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
pub fn row_probe_v1(input: Vec<u8>) -> Vec<u8> {
    if input.len() <= ROW_PROBE_PREFIX_BYTES {
        return input;
    }
    let digest = xxhash_rust::xxh3::xxh3_128_with_seed(&input, XXH3_128_SEED);
    let mut output = Vec::with_capacity(ROW_PROBE_PREFIX_BYTES + 16);
    output.extend_from_slice(&input[..ROW_PROBE_PREFIX_BYTES]);
    output.extend_from_slice(&(digest as u64).to_be_bytes());
    output.extend_from_slice(&((digest >> 64) as u64).to_be_bytes());
    output
}

fn datum_error(field: &str, error: impl fmt::Display) -> RowIdV2Error {
    RowIdV2Error::Datum {
        field: field.to_owned(),
        detail: error.to_string(),
    }
}

fn raw_varlena_payload(value: &RawElement) -> Vec<u8> {
    // SAFETY: PostgreSQL's type descriptor says this datum is a varlena value; detoasting
    // produces a readable pointer for the duration of this call.
    let varlena = unsafe { pg_sys::pg_detoast_datum_packed(value.0.datum().cast_mut_ptr()) };
    // SAFETY: `varlena` is the non-null pointer returned by PostgreSQL above.
    unsafe { pgrx::varlena_to_byte_slice(varlena) }.to_vec()
}

fn raw_fixed_payload(value: &RawElement, bytes: usize) -> Vec<u8> {
    // SAFETY: fixed-width pass-by-reference network types point to at least `bytes` readable
    // bytes, as guaranteed by PostgreSQL's type representation.
    unsafe { std::slice::from_raw_parts(value.0.datum().cast_mut_ptr::<u8>(), bytes).to_vec() }
}

fn encode_record_field(
    record: &PgHeapTuple<'_, AllocatedByRust>,
    attno: std::num::NonZeroUsize,
    field: &str,
    descriptor: &TypeDescriptor,
) -> Result<Option<Vec<u8>>, RowIdV2Error> {
    match descriptor.scalar {
        ScalarSupport::Bool => record
            .get_by_index::<bool>(attno)
            .map_err(|error| datum_error(field, error))
            .map(|value| value.map(|value| encode_bool(value).to_vec())),
        ScalarSupport::SignedInteger { bytes: 2 } => record
            .get_by_index::<i16>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| encode_signed(value as i128, 2).map(Some))
            }),
        ScalarSupport::SignedInteger { bytes: 4 } => record
            .get_by_index::<i32>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| encode_signed(value as i128, 4).map(Some))
            }),
        ScalarSupport::SignedInteger { bytes: 8 } => record
            .get_by_index::<i64>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| encode_signed(value as i128, 8).map(Some))
            }),
        ScalarSupport::UnsignedInteger { bytes: 4 } => record
            .get_by_index::<pgrx::pg_sys::Oid>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| {
                    encode_unsigned(value.to_u32() as u128, 4).map(Some)
                })
            }),
        ScalarSupport::Float { bytes: 4 } => record
            .get_by_index::<f32>(attno)
            .map_err(|error| datum_error(field, error))
            .map(|value| value.map(|value| encode_float32(value).to_vec())),
        ScalarSupport::Float { bytes: 8 } => record
            .get_by_index::<f64>(attno)
            .map_err(|error| datum_error(field, error))
            .map(|value| value.map(|value| encode_float64(value).to_vec())),
        ScalarSupport::Bytes => record
            .get_by_index::<RawElement>(attno)
            .map_err(|error| datum_error(field, error))
            .map(|value| value.map(|value| encode_bytes(&raw_varlena_payload(&value)))),
        ScalarSupport::Text => record
            .get_by_index::<RawElement>(attno)
            .map_err(|error| datum_error(field, error))
            .map(|value| {
                value.map(|value| {
                    let value = raw_varlena_payload(&value);
                    if descriptor.type_tag == TYPE_BPCHAR {
                        encode_bpchar(&value)
                    } else {
                        encode_text(&value)
                    }
                })
            }),
        ScalarSupport::Numeric => record
            .get_by_index::<RawElement>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| encode_numeric_datum(&value).map(Some))
            }),
        ScalarSupport::Date => record
            .get_by_index::<pgrx::datum::Date>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| {
                    encode_signed(i128::from(pg_sys::DateADT::from(value)), 4).map(Some)
                })
            }),
        ScalarSupport::Time => record
            .get_by_index::<pgrx::datum::Time>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| {
                    encode_unsigned(pg_sys::TimeADT::from(value) as u128, 8).map(Some)
                })
            }),
        ScalarSupport::Timestamp => record
            .get_by_index::<pgrx::datum::Timestamp>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| {
                    encode_signed(i128::from(pg_sys::Timestamp::from(value)), 8).map(Some)
                })
            }),
        ScalarSupport::TimestampWithTimeZone => record
            .get_by_index::<pgrx::datum::TimestampWithTimeZone>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| {
                    encode_signed(i128::from(pg_sys::TimestampTz::from(value)), 8).map(Some)
                })
            }),
        ScalarSupport::TimeWithTimeZone => record
            .get_by_index::<pgrx::datum::TimeWithTimeZone>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| {
                    let (time, zone) = <(pg_sys::TimeADT, i32)>::from(value);
                    Ok(Some(encode_timetz_value(time, zone).to_vec()))
                })
            }),
        ScalarSupport::Interval => record
            .get_by_index::<pgrx::datum::Interval>(attno)
            .map_err(|error| datum_error(field, error))
            .map(|value| {
                value.map(|value| {
                    encode_interval_value(value.months(), value.days(), value.micros()).to_vec()
                })
            }),
        ScalarSupport::Inet | ScalarSupport::Cidr => record
            .get_by_index::<RawElement>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| {
                    encode_network_payload(
                        &raw_varlena_payload(&value),
                        descriptor.type_tag == TYPE_CIDR,
                    )
                    .map(Some)
                })
            }),
        ScalarSupport::Macaddr { bytes } => record
            .get_by_index::<RawElement>(attno)
            .map_err(|error| datum_error(field, error))
            .map(|value| value.map(|value| raw_fixed_payload(&value, bytes as usize))),
        ScalarSupport::BitString => record
            .get_by_index::<RawElement>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| {
                    encode_bit_payload(&raw_varlena_payload(&value)).map(Some)
                })
            }),
        ScalarSupport::Enum => record
            .get_by_index::<RawElement>(attno)
            .map_err(|error| datum_error(field, error))
            .and_then(|value| {
                value.map_or(Ok(None), |value| {
                    // SAFETY: enum datums are pass-by-value OIDs and the descriptor was
                    // classified as an enum by PostgreSQL's type catalog.
                    let enum_oid = pg_sys::Oid::from(value.0.datum().value() as u32);
                    // SAFETY: SearchSysCache1 is called with a live enum OID and the returned
                    // tuple is released on every successful lookup path.
                    let tuple = unsafe {
                        pg_sys::SearchSysCache1(
                            pg_sys::SysCacheIdentifier::ENUMOID as i32,
                            pg_sys::Datum::from(enum_oid),
                        )
                    };
                    if tuple.is_null() {
                        return Err(RowIdV2Error::Datum {
                            field: field.to_owned(),
                            detail: format!(
                                "enum value OID {} is not in pg_enum",
                                enum_oid.to_u32()
                            ),
                        });
                    }
                    // SAFETY: SearchSysCache1 returned a valid pg_enum tuple.
                    let label = unsafe {
                        let form = pg_sys::GETSTRUCT(tuple).cast::<pg_sys::FormData_pg_enum>();
                        std::ffi::CStr::from_ptr((*form).enumlabel.data.as_ptr())
                            .to_bytes()
                            .to_owned()
                    };
                    // SAFETY: tuple was returned by SearchSysCache1 above.
                    unsafe { pg_sys::ReleaseSysCache(tuple) };
                    if label.len() > 63 {
                        return Err(RowIdV2Error::InvalidFieldMetadata {
                            field: field.to_owned(),
                            detail: "enum label exceeds 63 bytes".to_owned(),
                        });
                    }
                    Ok(Some(encode_bytes(&label)))
                })
            }),
        ScalarSupport::Uuid => record
            .get_by_index::<pgrx::Uuid>(attno)
            .map_err(|error| datum_error(field, error))
            .map(|value| value.map(|value| encode_uuid(value.as_bytes()).to_vec())),
        _ => Err(RowIdV2Error::InvalidFieldMetadata {
            field: field.to_owned(),
            detail: format!(
                "registry entry '{}' has no foundation encoder",
                descriptor.name
            ),
        }),
    }
}

fn encode_record(
    domain: &str,
    record: PgHeapTuple<'_, AllocatedByRust>,
) -> Result<Vec<u8>, RowIdV2Error> {
    let domain = IdentityDomain::from_str(domain)?;
    if record.len() > MAX_TUPLE_FIELDS {
        return Err(RowIdV2Error::TooManyFields {
            found: record.len(),
            maximum: MAX_TUPLE_FIELDS,
        });
    }
    let registry = TypeRegistry::new();
    let mut fields: Vec<(u8, Option<Vec<u8>>)> = Vec::with_capacity(record.len());
    for (attno, attribute) in record.attributes() {
        let field = attribute.name();
        if attribute.attisdropped {
            return Err(RowIdV2Error::InvalidFieldMetadata {
                field: field.to_owned(),
                detail: "dropped attributes are not identity fields".to_owned(),
            });
        }
        // SAFETY: PostgreSQL owns the catalog type OID and returns its registered base type.
        let oid = unsafe { pg_sys::getBaseType(attribute.atttypid) }.to_u32();
        let descriptor = match registry.validate_type(field, oid, 18) {
            Ok(descriptor) => descriptor,
            Err(RowIdV2Error::UnknownType { .. }) => {
                // SAFETY: `oid` came from the live tuple descriptor.
                let is_enum = unsafe { pg_sys::get_typtype(pg_sys::Oid::from(oid)) as u8 }
                    == pg_sys::TYPTYPE_ENUM;
                if is_enum {
                    &ENUM_DESCRIPTOR
                } else {
                    return Err(RowIdV2Error::UnknownType {
                        field: field.to_owned(),
                        oid,
                    });
                }
            }
            Err(error) => return Err(error),
        };
        if matches!(descriptor.scalar, ScalarSupport::Text)
            && attribute.attcollation != pgrx::pg_sys::InvalidOid
        {
            // SAFETY: `attcollation` comes from the live PostgreSQL tuple descriptor.
            let deterministic =
                unsafe { pgrx::pg_sys::get_collation_isdeterministic(attribute.attcollation) };
            validate_collation(field, deterministic)?;
        }
        let payload = encode_record_field(&record, attno, field, descriptor)?;
        fields.push((descriptor.type_tag, payload));
    }
    let mut output = Vec::with_capacity(6 + fields.len() * 2 + 1);
    output.push(IDENTITY_VERSION_V2);
    output.push(domain.tag());
    output.extend_from_slice(&(fields.len() as u32).to_be_bytes());
    for (type_tag, payload) in fields {
        output.push(type_tag);
        match payload {
            None => output.push(NULL_TAG),
            Some(payload) => {
                output.push(VALUE_TAG);
                output.extend_from_slice(&payload);
            }
        }
        if output.len() > MAX_ENCODED_IDENTITY_BYTES {
            return Err(RowIdV2Error::EncodedIdentityTooLarge {
                maximum: MAX_ENCODED_IDENTITY_BYTES,
            });
        }
    }
    if output.len() >= MAX_ENCODED_IDENTITY_BYTES {
        return Err(RowIdV2Error::EncodedIdentityTooLarge {
            maximum: MAX_ENCODED_IDENTITY_BYTES,
        });
    }
    output.push(TUPLE_END_TAG);
    Ok(output)
}

/// Encode a PostgreSQL record into exact V2 identity bytes.
#[pg_extern(schema = "pgtrickle", stable, parallel_safe)]
pub fn encode_row_id_v2(domain: &str, record: pgrx::AnyElement) -> Vec<u8> {
    let record: Option<PgHeapTuple<'_, AllocatedByRust>> =
        unsafe { PgHeapTuple::from_polymorphic_datum(record.datum(), false, record.oid()) };
    let result = record.ok_or_else(|| RowIdV2Error::InvalidFieldMetadata {
        field: "<record>".to_owned(),
        detail: "record argument is NULL".to_owned(),
    });
    match result.and_then(|record| encode_record(domain, record)) {
        Ok(identity) => identity,
        Err(error) => pgrx::error!("{}", error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_tags_and_domains() {
        assert_eq!(
            IdentityDomain::from_str("SCAN_KEY").unwrap().tag(),
            DOMAIN_SCAN_KEY
        );
        assert_eq!(
            IdentityDomain::from_str("SYNTHETIC").unwrap().tag(),
            DOMAIN_SYNTHETIC
        );
        assert_eq!(TYPE_BOOL, 1);
        assert_eq!(TYPE_COMPOSITE, 0x40);
    }

    #[test]
    fn tuple_framing_is_prefix_free() {
        let left = encode_bytes(b"ab");
        let right = encode_bytes(b"c");
        let first = encode_tuple(
            IdentityDomain::JoinKey,
            &[
                EncodedField::value(TYPE_TEXT, &left),
                EncodedField::value(TYPE_TEXT, &right),
            ],
        )
        .unwrap();
        let left = encode_bytes(b"a");
        let right = encode_bytes(b"bc");
        let second = encode_tuple(
            IdentityDomain::JoinKey,
            &[
                EncodedField::value(TYPE_TEXT, &left),
                EncodedField::value(TYPE_TEXT, &right),
            ],
        )
        .unwrap();
        assert_ne!(first, second);
        assert!(!first.starts_with(&second));
        assert!(!second.starts_with(&first));
        assert_eq!(&first[..6], &[2, DOMAIN_JOIN_KEY, 0, 0, 0, 2]);
    }

    #[test]
    fn scalar_transforms_canonicalize_special_values() {
        assert_eq!(encode_bool(false), [0]);
        assert_eq!(
            encode_signed_i64(-1),
            0x7fff_ffff_ffff_ffffu64.to_be_bytes()
        );
        assert_eq!(encode_unsigned_u64(42), 42u64.to_be_bytes());
        assert_eq!(encode_float64(0.0), encode_float64(-0.0));
        assert_eq!(
            encode_float64(f64::NAN),
            0xfff8_0000_0000_0000u64.to_be_bytes()
        );
        assert_eq!(
            encode_numeric_text("1.00").unwrap(),
            encode_numeric_text("1").unwrap()
        );
        assert!(encode_numeric_text("0.9").unwrap() < encode_numeric_text("1").unwrap());
        assert!(encode_numeric_text("1").unwrap() < encode_numeric_text("1.1").unwrap());
        assert!(encode_numeric_text("-1.1").unwrap() < encode_numeric_text("-1").unwrap());
        assert_eq!(encode_numeric_text("-0.00").unwrap(), [0x02]);
        assert_eq!(encode_numeric_text("-Infinity").unwrap(), [0x00]);
    }

    #[test]
    fn escaping_and_probe_boundaries() {
        assert_eq!(encode_bytes(&[0, 1, 255]), vec![0, 255, 1, 255, 0, 0]);
        let short = vec![1u8; ROW_PROBE_PREFIX_BYTES];
        assert_eq!(row_probe_v1(short.clone()), short);
        let long = vec![7u8; ROW_PROBE_PREFIX_BYTES + 1];
        assert_eq!(row_probe_v1(long).len(), ROW_PROBE_PREFIX_BYTES + 16);
    }

    #[test]
    fn temporal_network_and_bit_payloads_are_canonical() {
        assert_eq!(
            encode_interval_value(1, 0, 0),
            encode_interval_value(0, 30, 0)
        );
        let east = encode_timetz_value(3_600_000_000, 3_600);
        let utc = encode_timetz_value(7_200_000_000, 0);
        assert_eq!(east[..8], utc[..8]);
        assert_ne!(east, utc);
        assert_eq!(
            encode_network_payload(&[2, 24, 192, 0, 2, 1], true).unwrap(),
            vec![2, 24, 192, 0, 2, 0]
        );
        let mut bit_payload = 3i32.to_ne_bytes().to_vec();
        bit_payload.push(0b1010_0000);
        assert_eq!(
            encode_bit_payload(&bit_payload).unwrap(),
            vec![0, 0, 0, 3, 0b1010_0000, 0, 0]
        );
    }

    #[test]
    fn registry_supports_scalars_and_rejects_structural_types() {
        let registry = TypeRegistry::new();
        assert_eq!(registry.validate_type("id", 23, 18).unwrap().name, "int4");
        assert!(matches!(
            registry.validate_type("payload", 3802, 18),
            Err(RowIdV2Error::UnsupportedStructuralType { .. })
        ));
        assert!(matches!(
            registry.validate_type("payload", 999_999, 18),
            Err(RowIdV2Error::UnknownType { .. })
        ));
        assert!(matches!(
            registry.validate_type("id", 23, 17),
            Err(RowIdV2Error::UnsupportedPostgresMajor(17))
        ));
    }

    #[test]
    fn schema_validation_has_context() {
        assert!(validate_identity_schema("GROUP_KEY", &[23, 25]).is_ok());
        assert!(matches!(
            validate_identity_schema("GROUP_KEY", &[3802]),
            Err(RowIdV2Error::UnsupportedStructuralType { field, .. }) if field == "field 0"
        ));
        assert!(validate_collation("name", false).is_err());
    }

    #[test]
    fn source_key_policy_rejects_unsafe_unique_forms() {
        let valid = SourceKeyContract {
            primary: false,
            columns_not_null: false,
            nulls_not_distinct: true,
            partial: false,
            expression: false,
            deferrable: false,
            immediate: true,
            default_btree_operator_class: true,
        };
        assert!(validate_source_key("orders_order_id_key", valid).is_ok());
        assert!(matches!(
            validate_source_key(
                "orders_order_id_key",
                SourceKeyContract {
                    partial: true,
                    ..valid
                }
            ),
            Err(RowIdV2Error::InvalidSourceKey { .. })
        ));
        assert!(matches!(
            validate_source_key(
                "orders_order_id_key",
                SourceKeyContract {
                    columns_not_null: false,
                    nulls_not_distinct: false,
                    ..valid
                }
            ),
            Err(RowIdV2Error::InvalidSourceKey { .. })
        ));
    }
}

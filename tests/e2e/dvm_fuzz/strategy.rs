//! v0.87.6 COR-19: strategy, CDC, and refresh-mode axes for corpus replay.
//!
//! Covers two independently-verifiable execution axes that already have a
//! real, deterministic knob in the product:
//!   - refresh mode: DIFFERENTIAL vs FULL (`execution.requested_refresh_mode`
//!     / `expected_capability` on a `Scenario`)
//!   - CDC mode: trigger vs wal (the `pg_trickle.cdc_mode` GUC)
//!
//! ponytail: "apply variant" (prepared vs per-call connection) and "cache
//! variant" from COR-19's full item list are deliberately out of scope here
//! pending a real product-level hook to vary them safely; only the two axes
//! with a genuine, already-existing GUC/field are covered.
//!
//! This module is intentionally generic over the scenario shape: it takes no
//! dependency on the sibling `dvm_fuzz` module's `Scenario`/`ReplayFailure`
//! types (a `super::` import here would not resolve correctly when this file
//! and `dvm_fuzz/mod.rs` are each pulled into a test binary via independent
//! `#[path = ...]` module declarations with no real parent-child relationship
//! between them). Callers that need the actual `Scenario` mutation combine
//! these pure string-mapping helpers with their own copy of that logic.

#![allow(dead_code)]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshModeVariant {
    Differential,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcModeVariant {
    Trigger,
    Wal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategyCase {
    pub refresh_mode: RefreshModeVariant,
    pub cdc_mode: CdcModeVariant,
}

/// The 4 combinations of refresh mode x CDC mode, deterministic order.
pub fn all_variants() -> Vec<StrategyCase> {
    let refresh_modes = [RefreshModeVariant::Differential, RefreshModeVariant::Full];
    let cdc_modes = [CdcModeVariant::Trigger, CdcModeVariant::Wal];
    let mut out = Vec::with_capacity(4);
    for &refresh_mode in &refresh_modes {
        for &cdc_mode in &cdc_modes {
            out.push(StrategyCase {
                refresh_mode,
                cdc_mode,
            });
        }
    }
    out
}

/// The `execution.requested_refresh_mode` value for a given variant.
pub fn requested_refresh_mode(variant: RefreshModeVariant) -> &'static str {
    match variant {
        RefreshModeVariant::Differential => "DIFFERENTIAL",
        RefreshModeVariant::Full => "FULL",
    }
}

/// The `expected_capability.expected_mode` value for a given variant.
pub fn expected_mode(variant: RefreshModeVariant) -> &'static str {
    match variant {
        RefreshModeVariant::Differential => "DIFFERENTIAL",
        RefreshModeVariant::Full => "FULL",
    }
}

/// The SQL literal to pass as the value of `pg_trickle.cdc_mode`.
pub fn cdc_mode_literal(variant: CdcModeVariant) -> &'static str {
    match variant {
        CdcModeVariant::Trigger => "'trigger'",
        CdcModeVariant::Wal => "'wal'",
    }
}

/// The bare setting name `SHOW pg_trickle.cdc_mode` reports for a variant.
pub fn cdc_mode_name(variant: CdcModeVariant) -> &'static str {
    match variant {
        CdcModeVariant::Trigger => "trigger",
        CdcModeVariant::Wal => "wal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_variants_has_4_combinations() {
        let variants = all_variants();
        assert_eq!(variants.len(), 4);
        for (i, a) in variants.iter().enumerate() {
            for b in &variants[i + 1..] {
                assert_ne!(a, b, "all 4 combinations must be distinct");
            }
        }
    }

    #[test]
    fn refresh_mode_string_mappings_are_consistent() {
        assert_eq!(
            requested_refresh_mode(RefreshModeVariant::Differential),
            "DIFFERENTIAL"
        );
        assert_eq!(
            expected_mode(RefreshModeVariant::Differential),
            "DIFFERENTIAL"
        );
        assert_eq!(requested_refresh_mode(RefreshModeVariant::Full), "FULL");
        assert_eq!(expected_mode(RefreshModeVariant::Full), "FULL");
    }

    #[test]
    fn cdc_mode_string_mappings_are_consistent() {
        assert_eq!(cdc_mode_literal(CdcModeVariant::Trigger), "'trigger'");
        assert_eq!(cdc_mode_name(CdcModeVariant::Trigger), "trigger");
        assert_eq!(cdc_mode_literal(CdcModeVariant::Wal), "'wal'");
        assert_eq!(cdc_mode_name(CdcModeVariant::Wal), "wal");
    }
}

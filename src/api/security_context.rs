//! LSEC-1/LSEC-2: Security-context foundation (v0.87.7).
//!
//! All identity-switching `unsafe` code for the lifecycle-security programme
//! lives in this module. It captures the *original* caller's role and exact
//! pre-call `search_path`, and lets privileged code run a closure as a
//! stream table's storage owner, under that stored path, with guaranteed
//! restoration on every exit path.
//!
//! No public lifecycle function becomes `SECURITY DEFINER` in this release.
//! `create_stream_table` and its presets already were before this module
//! existed; this module only replaces the ad hoc `"$user", public` path
//! guess those entry points used with an exact, tested capture, and adds the
//! primitives later releases will use to execute defining SQL as the stream
//! owner instead of the extension owner.

use super::helpers::{outer_user_id, outer_user_name, quote_identifier};
use super::*;

unsafe extern "C" {
    fn find_option(
        name: *const std::os::raw::c_char,
        missing_ok: bool,
        is_assign: bool,
        elevel: i32,
    ) -> *mut std::os::raw::c_void;
}

#[repr(C)]
struct GucVariable {
    vartype: u32,
    stack: *mut GucStack,
}

#[repr(C)]
struct GucStack {
    prior: *mut GucStack,
    val: GucValue,
}

#[repr(C)]
union GucValue {
    int_val: i32,
    real_val: f64,
    bool_val: u8,
    string_val: *mut std::os::raw::c_char,
}

/// Which kind of public entry point captured a [`CallerContext`].
///
/// A `SecurityDefiner` entry has already had its `search_path` overwritten
/// by PostgreSQL's function-level `SET search_path = ...` by the time Rust
/// code runs, so the caller's real path must be recovered from the GUC
/// stack. A `SecurityInvoker` entry never had its path touched, so the
/// active GUC value already *is* the caller's path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum EntryContext {
    SecurityDefiner,
    #[default]
    SecurityInvoker,
}

/// The original caller's identity and exact `search_path`, captured at a
/// public entry point before any privileged work runs.
#[derive(Debug, Clone)]
pub(crate) struct CallerContext {
    pub role_oid: pg_sys::Oid,
    pub role_name: String,
    pub search_path: String,
}

/// The identity and stored path a stream table's defining SQL must run
/// under.
#[derive(Debug, Clone)]
pub(crate) struct StreamExecutionContext {
    pub owner_oid: pg_sys::Oid,
    pub search_path: String,
}

/// LSEC-1: Capture the original caller's role and exact `search_path`.
pub(crate) fn capture_caller_context(entry: EntryContext) -> Result<CallerContext, PgTrickleError> {
    let role_oid = outer_user_id();
    let role_name = outer_user_name()?;
    let raw_path = match entry {
        EntryContext::SecurityInvoker => active_search_path()?,
        EntryContext::SecurityDefiner => saved_pre_definer_search_path()?,
    };
    Ok(CallerContext {
        role_oid,
        search_path: expand_user_placeholder(&raw_path, &role_name),
        role_name,
    })
}

/// LSEC-3: Resolve the execution context for a stream table's stored
/// defining SQL: the storage table's *current* owner (never a caller
/// string) and the exact path that was captured when the query was last
/// (re)defined.
pub(crate) fn stream_execution_context(
    meta: &StreamTableMeta,
) -> Result<StreamExecutionContext, PgTrickleError> {
    let owner_oid = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT relowner FROM pg_catalog.pg_class WHERE oid = $1",
        &[meta.pgt_relid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InternalError(format!(
            "security context: storage relation for stream table {} (relid {}) is missing",
            meta.pgt_id,
            meta.pgt_relid.to_u32(),
        ))
    })?;

    Ok(StreamExecutionContext {
        owner_oid,
        search_path: meta.defining_search_path.clone(),
    })
}

fn active_search_path() -> Result<String, PgTrickleError> {
    Spi::get_one::<String>("SELECT current_setting('search_path')")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| {
            PgTrickleError::InternalError("security context: search_path GUC returned NULL".into())
        })
}

/// LSEC-1: Recover the caller's `search_path` from before a `SECURITY
/// DEFINER` entry point's own `SET search_path = ...` was applied.
///
/// PostgreSQL pushes the pre-call value onto the GUC's stack (as a
/// `GUC_SAVE` state entry) when it applies a function-level `SET` config
/// item, and pops/restores it automatically when the function returns. This
/// reads that still-pushed entry while it is live, so the exact path the
/// caller was using can be persisted for later name resolution. Fails
/// closed if the expected stack entry is absent — this must never silently
/// fall back to the pinned definer path.
fn saved_pre_definer_search_path() -> Result<String, PgTrickleError> {
    // SAFETY: `find_option` returns a pointer into PostgreSQL's static GUC
    // variable table for a name it recognizes; "search_path" is a built-in
    // GUC that is always registered, so the returned pointer (once checked
    // non-null) is valid for the life of the backend. We only dereference
    // it on the main backend thread, inside a running SQL call, and never
    // retain the pointer past this function.
    unsafe {
        let guc_var = find_option(c"search_path".as_ptr(), false, true, 0);
        if guc_var.is_null() {
            return Err(PgTrickleError::InternalError(
                "security context: search_path GUC is not registered".into(),
            ));
        }
        let guc_var = guc_var as *mut GucVariable;
        if (*guc_var).vartype != pg_sys::config_type::PGC_STRING as u32 {
            return Err(PgTrickleError::InternalError(
                "security context: search_path GUC has an unexpected type".into(),
            ));
        }
        let stack = (*guc_var).stack;
        if stack.is_null() {
            return Err(PgTrickleError::InternalError(
                "security context: expected a saved search_path GUC stack entry for a \
                 security-definer entry point, but none was pushed"
                    .into(),
            ));
        }
        let ptr = (*(*stack).prior).val.string_val;
        if ptr.is_null() {
            return Err(PgTrickleError::InternalError(
                "security context: saved search_path GUC stack entry has no string value".into(),
            ));
        }
        Ok(std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}

/// LSEC-1: Expand a standalone `$user` element of a `search_path` value to
/// the quoted caller role name. Every other element — including its own
/// quoting — is left untouched. Handles quoted identifiers, a comma
/// embedded inside a quoted schema name, escaped double quotes (`""`), and
/// whitespace around each comma-separated element.
pub(crate) fn expand_user_placeholder(path: &str, role_name: &str) -> String {
    let quoted_role = quote_identifier(role_name);
    split_search_path_elements(path)
        .into_iter()
        .map(|element| {
            if element == "$user" {
                quoted_role.clone()
            } else {
                element
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Split a `search_path`-style comma-separated list into trimmed elements,
/// treating text inside double quotes (with `""` as an escaped quote) as
/// opaque — a comma inside a quoted identifier does not split it.
fn split_search_path_elements(path: &str) -> Vec<String> {
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = path.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            current.push(c);
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
        } else if c == '"' {
            in_quotes = true;
            current.push(c);
        } else if c == ',' {
            elements.push(current.trim().to_string());
            current.clear();
        } else {
            current.push(c);
        }
    }
    elements.push(current.trim().to_string());
    elements
}

fn set_local_guc(name: &std::ffi::CStr, value: &str) -> Result<(), PgTrickleError> {
    let cvalue = std::ffi::CString::new(value).map_err(|_| {
        PgTrickleError::InternalError("security context: GUC value contains a NUL byte".into())
    })?;
    // SAFETY: `set_config_option` is PostgreSQL's standard GUC-assignment
    // entry point. `GUC_ACTION_LOCAL` scopes the change to the current
    // (sub)transaction nesting level, so it self-reverts if that level
    // aborts even if the Rust `finally` below never runs.
    unsafe {
        pg_sys::set_config_option(
            name.as_ptr(),
            cvalue.as_ptr(),
            pg_sys::GucContext::PGC_USERSET,
            pg_sys::GucSource::PGC_S_SESSION,
            pg_sys::GucAction::GUC_ACTION_LOCAL,
            true,
            pgrx::PgLogLevel::ERROR as i32,
            false,
        );
    }
    Ok(())
}

/// LSEC-2: Run `f` as the stream table's storage owner, under its stored
/// defining `search_path`, with `row_security = on`. Restores the previous
/// role, security flags, and GUCs on every exit path: success, a
/// PostgreSQL `ERROR` raised inside `f`, or a Rust panic.
///
/// `f` never receives the extension owner's identity — only the canonical
/// owner OID resolved from stream-table metadata by
/// [`stream_execution_context`]. There is no general "run as arbitrary
/// role" entry point.
pub(crate) fn with_stream_owner_context<T>(
    ctx: &StreamExecutionContext,
    f: impl FnOnce() -> Result<T, PgTrickleError>,
) -> Result<T, PgTrickleError> {
    use std::panic::AssertUnwindSafe;

    let prev_search_path = active_search_path()?;
    let prev_row_security = Spi::get_one::<String>("SELECT current_setting('row_security')")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or_else(|| "on".to_string());

    let mut save_userid = pg_sys::Oid::from(0u32);
    let mut save_sec_context: core::ffi::c_int = 0;
    // SAFETY: `GetUserIdAndSecContext`/`SetUserIdAndSecContext` are
    // PostgreSQL's standard mechanism for temporarily executing as a
    // different role — the same one `SECURITY DEFINER` function calls and
    // extensions such as dblink/postgres_fdw use to run as a foreign-server
    // owner. `SECURITY_RESTRICTED_OPERATION` additionally disables `SET
    // ROLE`/`SET SESSION AUTHORIZATION` for the duration, so owner-context
    // SQL cannot escalate further. Both calls run on the main backend
    // thread; identity is restored in `.finally()` below on every exit.
    unsafe {
        pg_sys::GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
        pg_sys::SetUserIdAndSecContext(
            ctx.owner_oid,
            save_sec_context
                | pg_sys::SECURITY_LOCAL_USERID_CHANGE as core::ffi::c_int
                | pg_sys::SECURITY_RESTRICTED_OPERATION as core::ffi::c_int,
        );
    }

    let setup = set_local_guc(c"search_path", &ctx.search_path)
        .and_then(|_| set_local_guc(c"row_security", "on"));
    if let Err(e) = setup {
        // SAFETY: restore identity before propagating — the closure below
        // never ran, so nothing else needs to unwind.
        unsafe {
            pg_sys::SetUserIdAndSecContext(save_userid, save_sec_context);
        }
        return Err(e);
    }

    debug_assert_eq!(
        unsafe { pg_sys::GetUserId() },
        ctx.owner_oid,
        "security context: active role does not match the stream table owner"
    );

    // SAFETY: `PgTryBuilder`'s `finally` runs on both the success and the
    // PostgreSQL ERROR path while the backend remains in a valid state,
    // which is what makes restoration here unconditional.
    unsafe {
        pgrx::PgTryBuilder::new(AssertUnwindSafe(f))
            .finally(move || {
                if let Ok(cs) = std::ffi::CString::new(prev_search_path.clone()) {
                    pg_sys::set_config_option(
                        c"search_path".as_ptr(),
                        cs.as_ptr(),
                        pg_sys::GucContext::PGC_USERSET,
                        pg_sys::GucSource::PGC_S_SESSION,
                        pg_sys::GucAction::GUC_ACTION_LOCAL,
                        true,
                        pgrx::PgLogLevel::ERROR as i32,
                        false,
                    );
                }
                if let Ok(cs) = std::ffi::CString::new(prev_row_security.clone()) {
                    pg_sys::set_config_option(
                        c"row_security".as_ptr(),
                        cs.as_ptr(),
                        pg_sys::GucContext::PGC_USERSET,
                        pg_sys::GucSource::PGC_S_SESSION,
                        pg_sys::GucAction::GUC_ACTION_LOCAL,
                        true,
                        pgrx::PgLogLevel::ERROR as i32,
                        false,
                    );
                }
                pg_sys::SetUserIdAndSecContext(save_userid, save_sec_context);
            })
            .execute()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_user_basic() {
        assert_eq!(
            expand_user_placeholder("$user, public", "alice"),
            "\"alice\", public"
        );
    }

    #[test]
    fn test_expand_user_quoted_role_name() {
        assert_eq!(
            expand_user_placeholder("$user, public", "Weird Name"),
            "\"Weird Name\", public"
        );
    }

    #[test]
    fn test_expand_user_absent_leaves_path_untouched() {
        assert_eq!(
            expand_user_placeholder("pgtrickle, public", "alice"),
            "pgtrickle, public"
        );
    }

    #[test]
    fn test_expand_user_quoted_dollar_user_is_not_expanded() {
        // A *quoted* "$user" is a literal schema name, not the placeholder.
        assert_eq!(
            expand_user_placeholder("\"$user\", public", "alice"),
            "\"$user\", public"
        );
    }

    #[test]
    fn test_expand_user_comma_inside_quoted_identifier() {
        assert_eq!(
            expand_user_placeholder("$user, \"schema, with, commas\"", "alice"),
            "\"alice\", \"schema, with, commas\""
        );
    }

    #[test]
    fn test_expand_user_escaped_quote_inside_identifier() {
        assert_eq!(
            expand_user_placeholder("$user, \"weird\"\"schema\"", "alice"),
            "\"alice\", \"weird\"\"schema\""
        );
    }

    #[test]
    fn test_expand_user_whitespace_around_elements() {
        assert_eq!(
            expand_user_placeholder("  $user  ,   public  ", "alice"),
            "\"alice\", public"
        );
    }

    #[test]
    fn test_expand_user_empty_element_preserved() {
        assert_eq!(
            expand_user_placeholder("$user, , public", "alice"),
            "\"alice\", , public"
        );
    }

    #[test]
    fn test_expand_user_multiple_occurrences() {
        assert_eq!(
            expand_user_placeholder("$user, public, $user", "alice"),
            "\"alice\", public, \"alice\""
        );
    }

    #[test]
    fn test_expand_user_role_name_needing_quote_escaping() {
        assert_eq!(
            expand_user_placeholder("$user, public", "weird\"role"),
            "\"weird\"\"role\", public"
        );
    }

    #[test]
    fn test_split_search_path_elements_single() {
        assert_eq!(
            split_search_path_elements("public"),
            vec!["public".to_string()]
        );
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod pg_tests {
    use super::*;
    use pgrx::prelude::*;

    /// Test-only SECURITY DEFINER probe that captures the caller's original
    /// search_path exactly as a real lifecycle entry point would, so LSEC-1's
    /// GUC-stack recovery can be proven against a real backend rather than
    /// only unit-tested in isolation.
    #[pg_extern(security_definer)]
    #[search_path(pg_catalog, pg_temp)]
    fn pgt_test_capture_definer_path() -> String {
        capture_caller_context(EntryContext::SecurityDefiner)
            .expect("capture must succeed inside a security-definer probe")
            .search_path
    }

    #[pg_test]
    fn test_capture_definer_path_recovers_caller_search_path() {
        Spi::run("CREATE SCHEMA IF NOT EXISTS lsec_probe_schema").unwrap();
        Spi::run("SET search_path = lsec_probe_schema, \"$user\", public").unwrap();

        let role_name = outer_user_name().expect("outer role must resolve");
        let expected = format!("lsec_probe_schema, \"{}\", public", role_name);

        let captured =
            Spi::get_one::<String>("SELECT security_context.pgt_test_capture_definer_path()")
                .unwrap()
                .expect("probe must return a value");

        assert_eq!(captured, expected);
    }

    #[pg_test]
    fn test_capture_definer_path_restores_caller_guc_after_call() {
        Spi::run("SET search_path = public, pg_catalog").unwrap();
        let _ = Spi::get_one::<String>("SELECT security_context.pgt_test_capture_definer_path()")
            .unwrap();

        // PostgreSQL itself restores the function-local SET on return —
        // this proves the probe didn't leak its own pinned path outward.
        let after = Spi::get_one::<String>("SELECT current_setting('search_path')")
            .unwrap()
            .unwrap();
        assert_eq!(after, "public, pg_catalog");
    }

    #[pg_test]
    fn test_with_stream_owner_context_runs_as_owner_and_restores() {
        Spi::run("CREATE ROLE lsec_probe_owner").unwrap();
        Spi::run("CREATE SCHEMA lsec_probe_owner_schema").unwrap();

        let outer_role = outer_user_name().expect("outer role must resolve");
        let owner_oid = Spi::get_one::<pg_sys::Oid>(
            "SELECT oid FROM pg_roles WHERE rolname = 'lsec_probe_owner'",
        )
        .unwrap()
        .unwrap();

        let ctx = StreamExecutionContext {
            owner_oid,
            search_path: "lsec_probe_owner_schema, pg_catalog".to_string(),
        };

        let (observed_role, observed_path) = with_stream_owner_context(&ctx, || {
            let role = Spi::get_one::<String>("SELECT current_user")
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap();
            let path = Spi::get_one::<String>("SELECT current_setting('search_path')")
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap();
            Ok((role, path))
        })
        .unwrap();

        assert_eq!(observed_role, "lsec_probe_owner");
        assert_eq!(observed_path, "lsec_probe_owner_schema, pg_catalog");

        let restored_role = Spi::get_one::<String>("SELECT current_user")
            .unwrap()
            .unwrap();
        assert_eq!(restored_role, outer_role);

        Spi::run("DROP SCHEMA lsec_probe_owner_schema").unwrap();
        Spi::run("DROP ROLE lsec_probe_owner").unwrap();
    }

    #[pg_test]
    fn test_with_stream_owner_context_restores_after_postgres_error() {
        Spi::run("CREATE ROLE lsec_probe_owner_err").unwrap();
        let outer_role = outer_user_name().expect("outer role must resolve");
        let owner_oid = Spi::get_one::<pg_sys::Oid>(
            "SELECT oid FROM pg_roles WHERE rolname = 'lsec_probe_owner_err'",
        )
        .unwrap()
        .unwrap();

        let ctx = StreamExecutionContext {
            owner_oid,
            search_path: "pg_catalog, pg_temp".to_string(),
        };

        let result = with_stream_owner_context(&ctx, || {
            Spi::run("SELECT 1/0").map_err(|e| PgTrickleError::SpiError(e.to_string()))
        });
        assert!(result.is_err());

        let restored_role = Spi::get_one::<String>("SELECT current_user")
            .unwrap()
            .unwrap();
        assert_eq!(restored_role, outer_role);

        Spi::run("DROP ROLE lsec_probe_owner_err").unwrap();
    }

    #[pg_test]
    fn test_with_stream_owner_context_nests_without_leaking_inner_state() {
        Spi::run("CREATE ROLE lsec_probe_outer").unwrap();
        Spi::run("CREATE ROLE lsec_probe_inner").unwrap();
        let starting_role = outer_user_name().expect("outer role must resolve");

        let outer_owner = Spi::get_one::<pg_sys::Oid>(
            "SELECT oid FROM pg_roles WHERE rolname = 'lsec_probe_outer'",
        )
        .unwrap()
        .unwrap();
        let inner_owner = Spi::get_one::<pg_sys::Oid>(
            "SELECT oid FROM pg_roles WHERE rolname = 'lsec_probe_inner'",
        )
        .unwrap()
        .unwrap();

        let outer_ctx = StreamExecutionContext {
            owner_oid: outer_owner,
            search_path: "public, pg_catalog".to_string(),
        };
        let inner_ctx = StreamExecutionContext {
            owner_oid: inner_owner,
            search_path: "pg_temp, pg_catalog".to_string(),
        };

        let observed_outer_after_inner = with_stream_owner_context(&outer_ctx, || {
            with_stream_owner_context(&inner_ctx, || {
                Spi::get_one::<String>("SELECT current_user")
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
            })?;

            Spi::get_one::<String>("SELECT current_user")
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        })
        .unwrap()
        .unwrap();

        assert_eq!(observed_outer_after_inner, "lsec_probe_outer");

        let restored_role = Spi::get_one::<String>("SELECT current_user")
            .unwrap()
            .unwrap();
        assert_eq!(restored_role, starting_role);

        Spi::run("DROP ROLE lsec_probe_outer").unwrap();
        Spi::run("DROP ROLE lsec_probe_inner").unwrap();
    }
}

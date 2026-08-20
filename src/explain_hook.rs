//! Optional PostgreSQL 18 EXPLAIN annotations for stream-table scans.

use pgrx::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;

thread_local! {
    static STREAM_ANNOTATIONS: RefCell<HashMap<i32, String>> = RefCell::new(HashMap::new());
}

static mut PREVIOUS_PER_PLAN: pg_sys::explain_per_plan_hook_type = None;
static mut PREVIOUS_PER_NODE: pg_sys::explain_per_node_hook_type = None;

pub(crate) fn register() {
    // SAFETY: PostgreSQL exposes these hook slots for extension registration;
    // assignment occurs once during backend initialization.
    unsafe {
        PREVIOUS_PER_PLAN = pg_sys::explain_per_plan_hook;
        PREVIOUS_PER_NODE = pg_sys::explain_per_node_hook;
        pg_sys::explain_per_plan_hook = Some(explain_per_plan);
        pg_sys::explain_per_node_hook = Some(explain_per_node);
    }
}

unsafe extern "C-unwind" fn explain_per_plan(
    plannedstmt: *mut pg_sys::PlannedStmt,
    into: *mut pg_sys::IntoClause,
    es: *mut pg_sys::ExplainState,
    query_string: *const std::ffi::c_char,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
) {
    STREAM_ANNOTATIONS.with(|cache| cache.borrow_mut().clear());
    if !crate::config::pg_trickle_explain_annotations() {
        call_previous_plan(plannedstmt, into, es, query_string, params, query_env);
        return;
    }
    if !plannedstmt.is_null() {
        // SAFETY: PostgreSQL invokes this hook with a live PlannedStmt and its
        // rtable remains valid for the duration of EXPLAIN formatting.
        let rtable =
            unsafe { pgrx::PgList::<pg_sys::RangeTblEntry>::from_pg((*plannedstmt).rtable) };
        let relids: Vec<pg_sys::Oid> = rtable
            .iter_ptr()
            .filter_map(|rte| {
                // SAFETY: iter_ptr yields non-null RangeTblEntry pointers from
                // PostgreSQL's planned statement range table.
                let rte = unsafe { rte.as_ref()? };
                (rte.relid != pg_sys::InvalidOid).then_some(rte.relid)
            })
            .collect();
        if !relids.is_empty() {
            let in_list = relids
                .iter()
                .map(|oid| oid.to_u32().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT pgt_relid, format('lag %s ms, last refresh %s, mode %s',\n\
                         COALESCE(EXTRACT(EPOCH FROM (now() - last_refresh_at)) * 1000, 0)::bigint,\n\
                         COALESCE(last_refresh_at::text, 'never'), refresh_mode)\n\
                   FROM pgtrickle.pgt_stream_tables WHERE pgt_relid IN ({in_list})"
            );
            let annotations_by_relid: Result<HashMap<pg_sys::Oid, String>, pgrx::spi::SpiError> =
                Spi::connect(|client| {
                    let mut rows = client.select(&sql, None, &[])?;
                    let mut annotations = HashMap::new();
                    for row in &mut rows {
                        let Ok(Some(relid)) = row.get::<pg_sys::Oid>(1) else {
                            continue;
                        };
                        if let Ok(Some(value)) = row.get::<String>(2) {
                            annotations.insert(relid, value);
                        }
                    }
                    Ok(annotations)
                });
            if let Ok(annotations_by_relid) = annotations_by_relid {
                let annotations = rtable
                    .iter_ptr()
                    .enumerate()
                    .filter_map(|(index, rte_ptr)| {
                        // SAFETY: iter_ptr yields non-null RangeTblEntry pointers from
                        // PostgreSQL's planned statement range table.
                        let rte = unsafe { rte_ptr.as_ref()? };
                        annotations_by_relid
                            .get(&rte.relid)
                            .map(|value| ((index + 1) as i32, value.clone()))
                    })
                    .collect();
                STREAM_ANNOTATIONS.with(|cache| *cache.borrow_mut() = annotations);
            }
        }
    }
    call_previous_plan(plannedstmt, into, es, query_string, params, query_env);
}

unsafe extern "C-unwind" fn explain_per_node(
    planstate: *mut pg_sys::PlanState,
    ancestors: *mut pg_sys::List,
    relationship: *const std::ffi::c_char,
    plan_name: *const std::ffi::c_char,
    es: *mut pg_sys::ExplainState,
) {
    if crate::config::pg_trickle_explain_annotations() && !planstate.is_null() {
        // SAFETY: PostgreSQL supplies a live PlanState during node formatting;
        // the plan pointer is checked before reading the common Plan prefix.
        let plan = unsafe { (*planstate).plan };
        if !plan.is_null() {
            let scanrelid = match unsafe { (*plan).type_ } {
                pg_sys::NodeTag::T_SeqScan
                | pg_sys::NodeTag::T_SampleScan
                | pg_sys::NodeTag::T_IndexScan
                | pg_sys::NodeTag::T_IndexOnlyScan
                | pg_sys::NodeTag::T_BitmapHeapScan
                | pg_sys::NodeTag::T_TidScan
                | pg_sys::NodeTag::T_ForeignScan => {
                    // SAFETY: These PostgreSQL scan nodes all begin with the
                    // Scan layout containing scanrelid.
                    unsafe { (*(plan as *const pg_sys::Scan)).scanrelid as i32 }
                }
                _ => 0,
            };
            if scanrelid > 0
                && let Some(value) =
                    STREAM_ANNOTATIONS.with(|cache| cache.borrow().get(&scanrelid).cloned())
            {
                let label = CString::new("pg_trickle");
                let value = CString::new(value);
                if let (Ok(label), Ok(value)) = (label, value) {
                    // SAFETY: ExplainState and both C strings are valid for the
                    // duration of the native property call.
                    unsafe { pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es) };
                }
            }
        }
    }
    call_previous_node(planstate, ancestors, relationship, plan_name, es);
}

fn call_previous_plan(
    plannedstmt: *mut pg_sys::PlannedStmt,
    into: *mut pg_sys::IntoClause,
    es: *mut pg_sys::ExplainState,
    query_string: *const std::ffi::c_char,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
) {
    // SAFETY: The saved pointer came from PostgreSQL's hook slot and is called
    // with the original arguments supplied by PostgreSQL.
    unsafe {
        if let Some(previous) = PREVIOUS_PER_PLAN {
            previous(plannedstmt, into, es, query_string, params, query_env);
        }
    }
}

fn call_previous_node(
    planstate: *mut pg_sys::PlanState,
    ancestors: *mut pg_sys::List,
    relationship: *const std::ffi::c_char,
    plan_name: *const std::ffi::c_char,
    es: *mut pg_sys::ExplainState,
) {
    // SAFETY: The saved pointer came from PostgreSQL's hook slot and is called
    // with the original arguments supplied by PostgreSQL.
    unsafe {
        if let Some(previous) = PREVIOUS_PER_NODE {
            previous(planstate, ancestors, relationship, plan_name, es);
        }
    }
}

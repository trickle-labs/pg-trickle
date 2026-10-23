//! Invertible numeric SUM state: finite subtotal and counts of special inputs.

/// Keep the subtotal separate: Infinity - Infinity cannot recover finite inputs.
pub(crate) fn state_sql(argument: &str) -> String {
    let admitted = format!("pg_catalog.pg_typeof({argument})::oid IN (700,701,1700)");
    format!(
        "CASE WHEN pg_catalog.pg_typeof(SUM({argument}))::oid IN (700,701,1700) THEN \
         pg_catalog.jsonb_build_object('finite', COALESCE(SUM(CASE WHEN \
         {admitted} THEN CASE WHEN ({argument})::text \
         IN ('NaN','Infinity','-Infinity') THEN NULL ELSE ({argument})::text::numeric END END), 0), \
         'nan',COUNT(CASE WHEN {admitted} AND ({argument})::text='NaN' THEN 1 END), \
         'pos',COUNT(CASE WHEN {admitted} AND ({argument})::text='Infinity' THEN 1 END), \
         'neg',COUNT(CASE WHEN {admitted} AND ({argument})::text='-Infinity' THEN 1 END)) END"
    )
}

pub(crate) fn merge_sql(old: &str, inserts: &str, deletes: &str) -> String {
    let fields = ["finite", "nan", "pos", "neg"]
        .map(|field| {
            format!(
                "'{field}', COALESCE(({old}->>'{field}')::numeric,0) \
                 + COALESCE(({inserts}->>'{field}')::numeric,0) \
                 - COALESCE(({deletes}->>'{field}')::numeric,0)"
            )
        })
        .join(", ");
    format!(
        "CASE WHEN pg_catalog.jsonb_typeof({inserts})='object' THEN pg_catalog.jsonb_build_object({fields}) END"
    )
}

pub(crate) fn value_sql(state: &str) -> String {
    format!(
        "CASE WHEN ({state}->>'nan')::numeric > 0 OR \
         (({state}->>'pos')::numeric > 0 AND ({state}->>'neg')::numeric > 0) THEN 'NaN' \
         WHEN ({state}->>'pos')::numeric > 0 THEN 'Infinity' \
         WHEN ({state}->>'neg')::numeric > 0 THEN '-Infinity' ELSE {state}->>'finite' END"
    )
}

pub(crate) fn has_special_sql(state: &str) -> String {
    format!(
        "(COALESCE(({state}->>'nan')::numeric,0) <> 0 OR \
         COALESCE(({state}->>'pos')::numeric,0) <> 0 OR \
         COALESCE(({state}->>'neg')::numeric,0) <> 0)"
    )
}

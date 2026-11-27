// Sub-module of src/refresh/merge — see mod.rs for overview.
#[allow(unused_imports)]
use super::*;

pub(crate) fn capture_delta_explain(schema: &str, name: &str, delta_sql: &str) {
    use std::path::PathBuf;

    let dir = PathBuf::from("/tmp/delta_plans");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        pgrx::warning!("[pg_trickle] PGS_PROFILE_DELTA: failed to create /tmp/delta_plans: {e}");
        return;
    }

    // Estimated plan under the actual generation settings; no duplicate execution.
    let explain_sql = format!(
        "EXPLAIN (FORMAT JSON) SELECT * FROM ({delta_sql}) __pgt_explain_d"
    );

    let plan_json = Spi::connect(|client| {
        let result = client
            .select(&explain_sql, None, &[])
            .map_err(|e| format!("SPI error in explain: {e}"))?;
        let mut lines = Vec::new();
        for row in result {
            let line: Option<pgrx::Json> = row.get(1).unwrap_or(None);
            if let Some(j) = line {
                lines.push(j.0.to_string());
            }
        }
        Ok::<String, String>(lines.join("\n"))
    });

    let plan_json = match plan_json {
        Ok(j) => j,
        Err(e) => {
            pgrx::warning!(
                "[pg_trickle] PGS_PROFILE_DELTA: EXPLAIN failed for {schema}.{name}: {e}"
            );
            return;
        }
    };

    // Write to /tmp/delta_plans/<schema>_<name>.json
    let safe_schema = schema.replace('"', "").replace('/', "_");
    let safe_name = name.replace('"', "").replace('/', "_");
    let path = dir.join(format!("{safe_schema}_{safe_name}.json"));
    if let Err(e) = std::fs::write(&path, &plan_json) {
        pgrx::warning!(
            "[pg_trickle] PGS_PROFILE_DELTA: failed to write {}: {e}",
            path.display()
        );
    } else {
        pgrx::debug1!(
            "[pg_trickle] PGS_PROFILE_DELTA: plan written to {}",
            path.display()
        );
    }
}

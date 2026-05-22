/// build.rs — pre-build script for pg_trickle.
///
/// Runs scripts/gen_test_schema.py to regenerate tests/generated/schema.rs
/// from the latest sql/archive/pg_trickle--<version>.sql.  This ensures the
/// test harness schema stays in sync with the archive SQL without manual edits.
///
/// The generated file is .gitignore'd; `cargo build` must run before
/// `cargo test` in fresh checkouts (the CI matrix already does this).
use std::path::Path;
use std::process::Command;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set");
    let manifest_path = Path::new(&manifest_dir);

    // Re-run this build script if the archive SQL or the generator changes.
    println!("cargo:rerun-if-changed=sql/archive");
    println!("cargo:rerun-if-changed=scripts/gen_test_schema.py");

    let generated_dir = manifest_path.join("tests").join("generated");
    std::fs::create_dir_all(&generated_dir).expect("failed to create tests/generated/");

    let out_path = generated_dir.join("schema.rs");
    let script = manifest_path.join("scripts").join("gen_test_schema.py");

    let status = Command::new("python3")
        .arg(&script)
        .current_dir(manifest_path)
        .stdout(
            std::fs::File::create(&out_path)
                .unwrap_or_else(|e| panic!("cannot create {}: {e}", out_path.display())),
        )
        .status()
        .unwrap_or_else(|e| panic!("failed to run gen_test_schema.py: {e}"));

    if !status.success() {
        panic!(
            "gen_test_schema.py exited with non-zero status; \
             tests/generated/schema.rs may be stale"
        );
    }
}

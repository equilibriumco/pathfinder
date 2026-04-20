//! Pathfinder build script.
//!
//! Just sets up `vergen_gitcl` to query our git information for the build.

pub fn main() {
    let force_version_env_var_name = "PATHFINDER_FORCE_VERSION";

    println!("cargo:rerun-if-env-changed={force_version_env_var_name}");

    if let Ok(version) = std::env::var(force_version_env_var_name) {
        if !version.is_empty() {
            println!("cargo:rustc-env=VERGEN_GIT_DESCRIBE={version}");
            return;
        }
    }

    const ENABLE_DIRTY: bool = true;
    const ENABLE_TAGS: bool = true;
    let gitcl = vergen_gitcl::GitclBuilder::default()
        .describe(ENABLE_TAGS, ENABLE_DIRTY, None)
        .build()
        .expect("vergen failed; this is probably due to missing .git directory");
    vergen_gitcl::Emitter::new()
        .add_instructions(&gitcl)
        .expect("vergen failed; this is probably due to missing .git directory")
        .fail_on_error()
        .emit()
        .expect("vergen failed; this is probably due to missing .git directory");
}

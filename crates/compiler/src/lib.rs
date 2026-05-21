use std::io::Write;

use anyhow::Context;
use pathfinder_common::class_definition::{self, SerializedCasmDefinition};
use pathfinder_common::{felt, CasmHash};
use pathfinder_crypto::Felt;

/// Resource limits for the compiler child process.
#[derive(Debug, Clone, Copy)]
pub struct ResourceLimits {
    /// Virtual memory limit for the child process, in bytes.
    pub memory_usage: u64,
    /// CPU time limit for the child process, in whole seconds.
    pub cpu_time: u64,
}

impl ResourceLimits {
    /// Recommended virtual memory limit for the compiler child process, in
    /// bytes.
    ///
    /// See [`Self::RECOMMENDED_MEMORY_USAGE_LIMIT_MIB`].
    pub const RECOMMENDED_MEMORY_USAGE_LIMIT: u64 =
        Self::RECOMMENDED_MEMORY_USAGE_LIMIT_MIB * 1024 * 1024;

    /// Recommended virtual memory limit for the compiler child process, in MiB.
    pub const RECOMMENDED_MEMORY_USAGE_LIMIT_MIB: u64 = 4 * 1024;

    /// Recommended CPU time limit for the compiler child process, in seconds.
    pub const RECOMMENDED_CPU_TIME_LIMIT: u64 = 10;

    /// Create a new [`ResourceLimits`] with the recommended limits.
    pub const fn recommended() -> Self {
        Self {
            memory_usage: Self::RECOMMENDED_MEMORY_USAGE_LIMIT,
            cpu_time: Self::RECOMMENDED_CPU_TIME_LIMIT,
        }
    }

    /// Create a new [`ResourceLimits`] with the limits suitable for executing
    /// many tests in parallel.
    pub fn for_test() -> Self {
        let cpu_time = std::thread::available_parallelism()
            .map(|n| n.get() as u64)
            .unwrap_or(Self::RECOMMENDED_CPU_TIME_LIMIT);

        Self {
            memory_usage: Self::RECOMMENDED_MEMORY_USAGE_LIMIT,
            cpu_time,
        }
    }

    /// Create a new [`ResourceLimits`] with custom limits.
    pub fn new(memory_usage: u64, cpu_time: u64) -> Self {
        Self {
            memory_usage,
            cpu_time,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum BlockifierLibfuncs {
    #[default]
    Audited,
    All,
    Experimental,
}

impl std::fmt::Display for BlockifierLibfuncs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BlockifierLibfuncs::Audited => "audited",
            BlockifierLibfuncs::All => "all",
            BlockifierLibfuncs::Experimental => "experimental",
        };
        write!(f, "{}", s)
    }
}

/// Compile a Sierra class definition into CASM using an isolated child process.
///
/// Runs the `pathfinder compile` command in a child process, passes
/// `sierra_definition` via stdin, and returns the serialized CASM output read
/// from stdout. Resource limits are applied before the child process starts to
/// prevent runaway compilation from consuming unbounded CPU or memory.
///
/// This is a blocking function. When used inside an async runtime, it should be
/// called on a blocking thread.
///
/// # Arguments
///
/// * `sierra_definition` — raw JSON-encoded Sierra class definition.
///
/// * `max_memory_usage` — virtual address space limit for the child process, in
///   **bytes**. See [`ResourceLimits::RECOMMENDED_MEMORY_USAGE_LIMIT`].
///
/// * `max_cpu_time` — CPU time limit for the child process, in **whole
///   seconds**. See [`ResourceLimits::RECOMMENDED_CPU_TIME_LIMIT`].
///
/// # Platform support
///
/// Resource limits are applied via `setrlimit(2)` and are **Unix-only**. On
/// non-Unix platforms no resource limits are applied; the child process still
/// runs but without any resource constraints.
pub fn compile_sierra_to_casm(
    sierra_definition: &class_definition::SerializedSierraDefinition,
    resource_limits: ResourceLimits,
    blockifier_libfuncs: BlockifierLibfuncs,
) -> anyhow::Result<SerializedCasmDefinition> {
    let mut pathfinder_cmd = pathfinder_exe()
        .context("reading pathfinder executable path")
        .map(std::process::Command::new)?;

    pathfinder_cmd
        .arg("compile")
        .arg("--blockifier.libfunc-list")
        .arg(blockifier_libfuncs.to_string())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = set_resource_limits(pathfinder_cmd, resource_limits)
        .context("setting resource limits")?
        .spawn()
        .context("spawning compiler child process")?;

    {
        let mut ch_stdin = child.stdin.take().context("opening child stdin")?;
        ch_stdin
            .write_all(sierra_definition.as_slice())
            .context("writing Sierra definition to child stdin")?;
        ch_stdin.flush().context("flushing child stdin")?;
    }

    let output = child
        .wait_with_output()
        .context("waiting for compiler child process")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "compiler child process failed with status: {}; stderr: {}",
            output.status,
            stderr
        );
    }

    Ok(SerializedCasmDefinition::from_vec(output.stdout))
}

/// Compile a Sierra class definition into CASM using an isolated child process.
///
/// Because the definition is serialized before being to the child process,
/// there is some extra overhead compared to [`compile_sierra_to_casm`]. If
/// possible, try to use [`compile_sierra_to_casm`] directly to avoid this
/// overhead.
///
/// For more details see [`compile_sierra_to_casm`].
pub fn compile_sierra_to_casm_deser(
    sierra_definition: class_definition::Sierra<'_>,
    resource_limits: ResourceLimits,
    blockifier_libfuncs: BlockifierLibfuncs,
) -> anyhow::Result<SerializedCasmDefinition> {
    serde_json::to_vec(&sierra_definition)
        .context("serializing Sierra definition")
        .map(|sierra_definition| {
            compile_sierra_to_casm(
                &class_definition::SerializedSierraDefinition::from_vec(sierra_definition),
                resource_limits,
                blockifier_libfuncs,
            )
        })?
}

/// Get the path to the `pathfinder` executable.
///
/// In release builds, we assume this is the path to the current executable. In
/// debug builds, the previous assumption doesn't hold because we could either
/// be running the debug build of `pathfinder` or running from within `cargo
/// test`. Tricky.
fn pathfinder_exe() -> anyhow::Result<std::path::PathBuf> {
    let current_exe = std::env::current_exe().context("reading current executable path")?;
    let is_pathfinder_exe = current_exe
        .file_stem()
        .is_some_and(|name| name == "pathfinder");

    #[cfg(not(debug_assertions))]
    {
        if is_pathfinder_exe {
            // We're inside the release build of `pathfinder`, so we can just run the
            // current executable.
            Ok(current_exe)
        } else {
            // We're running a release build of a different executable, which is not
            // supported for now.
            anyhow::bail!(
                "In release builds, the compiler can only be used from the `pathfinder` \
                 executable. Current executable: {:?}",
                current_exe
            );
        }
    }
    #[cfg(debug_assertions)]
    {
        if is_pathfinder_exe {
            // We're inside the debug build of `pathfinder`, so we can just run the current
            // executable.
            Ok(current_exe)
        } else {
            // We're probably running from `cargo test`, so we need to find the `pathfinder`
            // executable in the target directory. If this fails, we're running a debug
            // build of a different executable, which won't be supported for now.
            let debug_dir = current_exe
                .parent() // target/debug/deps
                .context("getting deps directory")?
                .parent() // target/debug
                .context("getting debug directory")?;

            let pathfinder_exe = debug_dir.join("pathfinder");
            anyhow::ensure!(
                pathfinder_exe.exists(),
                "pathfinder executable not found in target directory: {:?}",
                pathfinder_exe
            );

            Ok(pathfinder_exe)
        }
    }
}

/// Set resource limits for the child process using `setrlimit(2)` on Unix.
///
/// Returns a [`spawn::Spawn`] which can be used to spawn the resource-limited
/// child process.
#[cfg(unix)]
fn set_resource_limits(
    mut cmd: std::process::Command,
    resource_limits: ResourceLimits,
) -> anyhow::Result<spawn::Spawn> {
    use std::os::unix::process::CommandExt;

    // SAFETY: We call `libc::setrlimit` with valid arguments. We do not use
    // any parent process memory, do not allocate memory, access environment
    // variables etc. inside the closure.
    unsafe {
        cmd.pre_exec(move || {
            #[cfg(target_os = "linux")]
            {
                let mem_limit = libc::rlimit {
                    rlim_cur: resource_limits.memory_usage,
                    rlim_max: resource_limits.memory_usage,
                };
                if libc::setrlimit(libc::RLIMIT_AS, &mem_limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }

            let cpu_limit = libc::rlimit {
                rlim_cur: resource_limits.cpu_time,
                rlim_max: resource_limits.cpu_time,
            };
            if libc::setrlimit(libc::RLIMIT_CPU, &cpu_limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }

            Ok(())
        });
    }

    Ok(spawn::Spawn::new(cmd))
}

/// Set resource limits for the child process on non-Unix platforms.
///
/// # Critical
///
/// Resource limits are **not supported** on non-Unix platforms. This function
/// logs a warning and returns the command unmodified, allowing the child
/// process to run without any resource constraints.
#[cfg(not(unix))]
fn set_resource_limits(
    cmd: std::process::Command,
    _: ResourceLimits,
) -> anyhow::Result<spawn::Spawn> {
    tracing::warn!(
        "Resource limits are not supported on this platform. Running `pathfinder-compiler` \
         without resource limits."
    );

    Ok(spawn::Spawn::new(cmd))
}

// The extra module will hide the `Command` to prevent misuse before spawning.
mod spawn {
    /// A helper struct to allow spawning the child process after setting
    /// resource limits while preventing misuse in the meantime.
    pub struct Spawn(std::process::Command);

    impl Spawn {
        pub fn new(cmd: std::process::Command) -> Self {
            Self(cmd)
        }

        pub fn spawn(&mut self) -> std::io::Result<std::process::Child> {
            self.0.spawn()
        }
    }
}

/// Compile a serialized Sierra class definition into CASM.
///
/// Calling this function directly will compile the Sierra class in-process,
/// which is not recommended. Use [`compile_sierra_to_casm`] to compile in an
/// isolated child process with resource limits.
pub fn compile_sierra_to_casm_impl(
    sierra_definition: &class_definition::SerializedSierraDefinition,
    blockifier_libfuncs: BlockifierLibfuncs,
    max_bytecode_size: usize,
) -> anyhow::Result<SerializedCasmDefinition> {
    // The class representation expected by the compiler doesn't match the
    // representation used by the feeder gateway for Sierra classes, so we have to
    // convert the JSON to something that can be parsed into the expected input
    // format for the compiler.
    serde_json::from_slice::<class_definition::Sierra<'_>>(sierra_definition.as_slice())
        .context("Parsing Sierra class")
        .map(|sierra_class| {
            compile_sierra_to_casm_deser_impl(sierra_class, blockifier_libfuncs, max_bytecode_size)
        })?
        .context("Compiling Sierra to CASM")
}

/// Compile a deserialized Sierra class definition into CASM.
///
/// Calling this function directly will compile the Sierra class in-process,
/// which is not recommended. Use [`compile_sierra_to_casm_deser`] to compile
/// in an isolated child process with resource limits.
pub fn compile_sierra_to_casm_deser_impl(
    sierra_definition: class_definition::Sierra<'_>,
    blockifier_libfuncs: BlockifierLibfuncs,
    max_bytecode_size: usize,
) -> anyhow::Result<SerializedCasmDefinition> {
    let version = parse_sierra_version(&sierra_definition.sierra_program)
        .context("Parsing Sierra version")?;

    let started_at = std::time::Instant::now();
    let result = std::panic::catch_unwind(|| match version {
        SierraVersion(0, 1, 0) => v1_0_0_alpha6::compile(sierra_definition),
        SierraVersion(1, 0, 0) => v1_0_0_rc0::compile(sierra_definition),
        SierraVersion(1, 1, 0) => v1_1_1::compile(sierra_definition),
        _ => v2::compile(sierra_definition, blockifier_libfuncs, max_bytecode_size),
    });
    tracing::trace!(elapsed=?started_at.elapsed(), "Sierra class compilation finished");

    result.unwrap_or_else(|e| Err(panic_error(e)))
}

fn panic_error(e: Box<dyn std::any::Any>) -> anyhow::Error {
    match e.downcast_ref::<&str>() {
        Some(e) => anyhow::anyhow!("Compiler panicked: {e}"),
        None => match e.downcast_ref::<String>() {
            Some(e) => anyhow::anyhow!("Compiler panicked: {e}"),
            None => anyhow::anyhow!("Compiler panicked"),
        },
    }
}

#[derive(Debug, PartialEq)]
struct SierraVersion(u64, u64, u64);

/// Parse Sierra version from a [Felt] slice representation of the program.
///
/// Sierra programs contain the version number in two possible formats.
/// For pre-1.0-rc0 Cairo versions the program contains the Sierra version
/// "0.1.0" as a shortstring in its first Felt (0x302e312e30 = "0.1.0").
/// For all subsequent versions the version number is the first three felts
/// representing the three parts of a semantic version number.
fn parse_sierra_version(program: &[Felt]) -> anyhow::Result<SierraVersion> {
    const VERSION_0_1_0_AS_SHORTSTRING: Felt = felt!("0x302e312e30");

    match program {
        [VERSION_0_1_0_AS_SHORTSTRING, ..] => Ok(SierraVersion(0, 1, 0)),
        [a, b, c, ..] => {
            let (a, b, c) = ((*a).try_into()?, (*b).try_into()?, (*c).try_into()?);

            Ok(SierraVersion(a, b, c))
        }
        _ => Err(anyhow::anyhow!("Invalid version felts")),
    }
}

/// Parse CASM class definition and return _Blake2_ CASM class hash.
///
/// Uses the _latest_ compiler for the parsing and starknet_api for the hashing.
pub fn casm_class_hash_v2(casm_definition: &SerializedCasmDefinition) -> anyhow::Result<CasmHash> {
    v2::casm_class_hash_v2(casm_definition.as_slice())
}

mod v1_0_0_alpha6 {
    use anyhow::Context;
    use casm_compiler_v1_0_0_alpha6::allowed_libfuncs::{
        validate_compatible_sierra_version,
        ListSelector,
    };
    use casm_compiler_v1_0_0_alpha6::casm_contract_class::CasmContractClass;
    use casm_compiler_v1_0_0_alpha6::contract_class::ContractClass;
    use pathfinder_common::class_definition;

    pub(super) fn pathfinder_to_starknet_contract_class(
        definition: class_definition::Sierra<'_>,
    ) -> Result<ContractClass, serde_json::Error> {
        let json = serde_json::json!({
            "abi": [],
            "sierra_program": definition.sierra_program,
            "contract_class_version": definition.contract_class_version,
            "entry_points_by_type": definition.entry_points_by_type,
        });
        serde_json::from_value::<ContractClass>(json)
    }

    pub(super) fn compile(
        definition: class_definition::Sierra<'_>,
    ) -> anyhow::Result<super::SerializedCasmDefinition> {
        let sierra_class: ContractClass = pathfinder_to_starknet_contract_class(definition)
            .context("Converting to Sierra class")?;

        validate_compatible_sierra_version(
            &sierra_class,
            ListSelector::ListName(
                // Keeping the "experimental" list for backwards
                // compatibility. Also, the names in
                // BlockifierLibfuncs would have to be mapped to
                // (audited|testnet|experimental)_v0.1.0 .
                casm_compiler_v1_0_0_alpha6::allowed_libfuncs::DEFAULT_EXPERIMENTAL_LIBFUNCS_LIST
                    .to_string(),
            ),
        )
        .context("Validating Sierra class")?;

        let casm_class = CasmContractClass::from_contract_class(sierra_class, true)
            .context("Compiling to CASM")?;
        Ok(super::SerializedCasmDefinition::from_vec(
            serde_json::to_vec(&casm_class)?,
        ))
    }
}

mod v1_0_0_rc0 {
    use anyhow::Context;
    use casm_compiler_v1_0_0_rc0::allowed_libfuncs::{
        validate_compatible_sierra_version,
        ListSelector,
    };
    use casm_compiler_v1_0_0_rc0::casm_contract_class::CasmContractClass;
    use casm_compiler_v1_0_0_rc0::contract_class::ContractClass;
    use pathfinder_common::class_definition;

    pub(super) fn pathfinder_to_starknet_contract_class(
        definition: class_definition::Sierra<'_>,
    ) -> Result<ContractClass, serde_json::Error> {
        let json = serde_json::json!({
            "abi": [],
            "sierra_program": definition.sierra_program,
            "contract_class_version": definition.contract_class_version,
            "entry_points_by_type": definition.entry_points_by_type,
        });
        serde_json::from_value::<ContractClass>(json)
    }

    pub(super) fn compile(
        definition: class_definition::Sierra<'_>,
    ) -> anyhow::Result<super::SerializedCasmDefinition> {
        let sierra_class: ContractClass = pathfinder_to_starknet_contract_class(definition)
            .context("Converting to Sierra class")?;

        validate_compatible_sierra_version(
            &sierra_class,
            ListSelector::ListName(
                // Keeping the "experimental" list for backwards
                // compatibility. Also, the names in
                // BlockifierLibfuncs would have to be mapped to
                // (audited|testnet|experimental)_v0.1.0 .
                casm_compiler_v1_0_0_rc0::allowed_libfuncs::DEFAULT_EXPERIMENTAL_LIBFUNCS_LIST
                    .to_string(),
            ),
        )
        .context("Validating Sierra class")?;

        let casm_class = CasmContractClass::from_contract_class(sierra_class, true)
            .context("Compiling to CASM")?;
        Ok(super::SerializedCasmDefinition::from_vec(
            serde_json::to_vec(&casm_class)?,
        ))
    }
}

mod v1_1_1 {
    use anyhow::Context;
    use casm_compiler_v1_1_1::allowed_libfuncs::{
        validate_compatible_sierra_version,
        ListSelector,
    };
    use casm_compiler_v1_1_1::casm_contract_class::CasmContractClass;
    use casm_compiler_v1_1_1::contract_class::ContractClass;
    use pathfinder_common::class_definition;

    pub(super) fn pathfinder_to_starknet_contract_class(
        definition: class_definition::Sierra<'_>,
    ) -> Result<ContractClass, serde_json::Error> {
        let json = serde_json::json!({
            "abi": [],
            "sierra_program": definition.sierra_program,
            "contract_class_version": definition.contract_class_version,
            "entry_points_by_type": definition.entry_points_by_type,
        });
        serde_json::from_value::<ContractClass>(json)
    }

    pub(super) fn compile(
        definition: class_definition::Sierra<'_>,
    ) -> anyhow::Result<super::SerializedCasmDefinition> {
        let sierra_class: ContractClass = pathfinder_to_starknet_contract_class(definition)
            .context("Converting to Sierra class")?;

        validate_compatible_sierra_version(
            &sierra_class,
            ListSelector::ListName(
                // Keeping the "experimental" list for backwards
                // compatibility. Also, the names in
                // BlockifierLibfuncs would have to be mapped to
                // (audited|testnet|experimental)_v0.1.0 .
                casm_compiler_v1_0_0_rc0::allowed_libfuncs::DEFAULT_EXPERIMENTAL_LIBFUNCS_LIST
                    .to_string(),
            ),
        )
        .context("Validating Sierra class")?;

        let casm_class = CasmContractClass::from_contract_class(sierra_class, true)
            .context("Compiling to CASM")?;
        Ok(super::SerializedCasmDefinition::from_vec(
            serde_json::to_vec(&casm_class)?,
        ))
    }
}

// This compiler is backwards compatible with v1.1.
mod v2 {
    use anyhow::Context;
    use cairo_lang_starknet_classes::casm_contract_class::CasmContractClass;
    use cairo_lang_starknet_classes::contract_class::ContractClass;

    use super::{BlockifierLibfuncs, CasmHash};

    pub(super) fn pathfinder_to_starknet_contract_class(
        definition: crate::class_definition::Sierra<'_>,
    ) -> Result<ContractClass, serde_json::Error> {
        let json = serde_json::json!({
            "abi": [],
            "sierra_program": definition.sierra_program,
            "contract_class_version": definition.contract_class_version,
            "entry_points_by_type": definition.entry_points_by_type,
        });
        serde_json::from_value::<ContractClass>(json)
    }

    pub(super) fn compile(
        definition: crate::class_definition::Sierra<'_>,
        blockifier_libfuncs: BlockifierLibfuncs,
        max_bytecode_size: usize,
    ) -> anyhow::Result<super::SerializedCasmDefinition> {
        let sierra_class: ContractClass = pathfinder_to_starknet_contract_class(definition)
            .context("Converting to Sierra class")?;

        let extracted_sierra_program = sierra_class
            .extract_sierra_program(false)
            .context("Extracting Sierra program")?;

        extracted_sierra_program
            .validate_version_compatible(
                cairo_lang_starknet_classes::allowed_libfuncs::ListSelector::ListName(
                    blockifier_libfuncs.to_string(),
                ),
            )
            .context("Validating Sierra class")?;

        let casm_class = CasmContractClass::from_contract_class(
            sierra_class,
            extracted_sierra_program,
            true,
            max_bytecode_size,
        )
        .context("Compiling to CASM")?;
        Ok(super::SerializedCasmDefinition::from_vec(
            serde_json::to_vec(&casm_class)?,
        ))
    }

    pub(super) fn casm_class_hash_v2(casm_definition: &[u8]) -> anyhow::Result<CasmHash> {
        let ccc: CasmContractClass =
            serde_json::from_slice(casm_definition).context("Deserializing CASM class")?;

        use starknet_api::contract_class::compiled_class_hash::{
            HashVersion,
            HashableCompiledClass,
        };

        let casm_hash_v2 = ccc.hash(&HashVersion::V2);

        let casm_hash_v2 = CasmHash(pathfinder_crypto::Felt::from_be_bytes(
            casm_hash_v2.0.to_bytes_be(),
        )?);

        Ok(casm_hash_v2)
    }
}

#[cfg(test)]
mod tests {
    use super::compile_sierra_to_casm_impl;

    mod parse_version {
        use pathfinder_common::class_definition;
        use rstest::rstest;
        use starknet_gateway_test_fixtures::class_definitions::{
            CAIRO_1_0_0_ALPHA5_SIERRA,
            CAIRO_1_0_0_RC0_SIERRA,
            CAIRO_1_1_0_RC0_SIERRA,
            CAIRO_2_0_0_STACK_OVERFLOW,
        };

        use super::super::{parse_sierra_version, SierraVersion};

        #[rstest]
        #[case(CAIRO_1_0_0_ALPHA5_SIERRA, SierraVersion(0, 1, 0))]
        #[case(CAIRO_1_0_0_RC0_SIERRA, SierraVersion(1, 0, 0))]
        #[case(CAIRO_1_1_0_RC0_SIERRA, SierraVersion(1, 1, 0))]
        #[case(CAIRO_2_0_0_STACK_OVERFLOW, SierraVersion(1, 2, 0))]
        fn parse_version(#[case] sierra_json: &[u8], #[case] expected_version: SierraVersion) {
            let sierra =
                serde_json::from_slice::<class_definition::Sierra<'_>>(sierra_json).unwrap();
            let sierra_version = parse_sierra_version(&sierra.sierra_program).unwrap();
            assert_eq!(sierra_version, expected_version);
        }
    }

    mod starknet_v0_11_0 {
        use pathfinder_common::class_definition;
        use starknet_gateway_test_fixtures::class_definitions::CAIRO_1_0_0_ALPHA5_SIERRA;

        use super::*;
        use crate::v1_0_0_rc0::pathfinder_to_starknet_contract_class;
        use crate::BlockifierLibfuncs;

        #[test]
        fn test_feeder_gateway_contract_conversion() {
            let class =
                serde_json::from_slice::<class_definition::Sierra<'_>>(CAIRO_1_0_0_ALPHA5_SIERRA)
                    .unwrap();

            let _: casm_compiler_v1_0_0_rc0::contract_class::ContractClass =
                pathfinder_to_starknet_contract_class(class).unwrap();
        }

        #[test]
        fn test_compile_ser() {
            compile_sierra_to_casm_impl(
                &class_definition::SerializedSierraDefinition::from_slice(
                    CAIRO_1_0_0_ALPHA5_SIERRA,
                ),
                BlockifierLibfuncs::default(),
                // Note: max_bytecode_size is relevant only for >=v2 compiler
                0usize,
            )
            .unwrap();
        }
    }

    mod starknet_v0_11_1 {
        use pathfinder_common::class_definition;
        use starknet_gateway_test_fixtures::class_definitions::CAIRO_1_0_0_RC0_SIERRA;

        use super::*;
        use crate::v1_0_0_rc0::pathfinder_to_starknet_contract_class;
        use crate::BlockifierLibfuncs;

        #[test]
        fn test_feeder_gateway_contract_conversion() {
            let class =
                serde_json::from_slice::<class_definition::Sierra<'_>>(CAIRO_1_0_0_RC0_SIERRA)
                    .unwrap();

            let _: casm_compiler_v1_0_0_rc0::contract_class::ContractClass =
                pathfinder_to_starknet_contract_class(class).unwrap();
        }

        #[test]
        fn test_compile_ser() {
            compile_sierra_to_casm_impl(
                &class_definition::SerializedSierraDefinition::from_slice(CAIRO_1_0_0_RC0_SIERRA),
                BlockifierLibfuncs::default(),
                // Note: max_bytecode_size is relevant only for >=v2 compiler
                0usize,
            )
            .unwrap();
        }
    }

    mod starknet_v0_11_2_onwards {
        use pathfinder_common::class_definition;
        use starknet_gateway_test_fixtures::class_definitions::{
            CAIRO_1_1_0_RC0_SIERRA,
            CAIRO_2_0_0_STACK_OVERFLOW,
        };

        use super::*;
        use crate::v2::pathfinder_to_starknet_contract_class;
        use crate::BlockifierLibfuncs;

        #[test]
        fn test_feeder_gateway_contract_conversion() {
            let class =
                serde_json::from_slice::<class_definition::Sierra<'_>>(CAIRO_1_1_0_RC0_SIERRA)
                    .unwrap();

            let _: cairo_lang_starknet_classes::contract_class::ContractClass =
                pathfinder_to_starknet_contract_class(class).unwrap();
        }

        #[test]
        fn test_compile_ser() {
            compile_sierra_to_casm_impl(
                &class_definition::SerializedSierraDefinition::from_slice(CAIRO_1_1_0_RC0_SIERRA),
                BlockifierLibfuncs::default(),
                // Note: max_bytecode_size is relevant only for >=v2 compiler
                0,
            )
            .unwrap();
        }

        #[test]
        fn regression_stack_overflow() {
            // This class caused a stack-overflow in v2 compilers <= v2.0.1
            compile_sierra_to_casm_impl(
                &class_definition::SerializedSierraDefinition::from_slice(
                    CAIRO_2_0_0_STACK_OVERFLOW,
                ),
                BlockifierLibfuncs::default(),
                // The default for mainnet & sepolia
                81920,
            )
            .unwrap();
        }
    }
}

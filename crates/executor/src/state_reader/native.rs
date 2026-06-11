use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use blockifier::execution::native::contract_class::NativeCompiledClassV1;
use blockifier::state::errors::StateError;
use cached::{Cached, SizedCache};
use cairo_native::executor::AotContractExecutor;
use cairo_vm::types::errors::program_errors::ProgramError;
use pathfinder_common::class_definition::{
    SerializedCasmDefinition,
    SerializedOpaqueClassDefinition,
};
use pathfinder_common::ClassHash;
use starknet_api::contract_class::SierraVersion;
use tokio_util::sync::CancellationToken;

struct CompilerInput {
    class_hash: ClassHash,
    sierra_version: SierraVersion,
    class_definition: SerializedOpaqueClassDefinition,
    casm_definition: SerializedCasmDefinition,
}

enum CacheItem {
    CompiledClass(NativeCompiledClassV1),
    CompilationPending,
}

type Cache = Mutex<SizedCache<ClassHash, CacheItem>>;

#[derive(Clone)]
pub struct NativeClassCache {
    inner: Arc<Inner>,
}

/// Owns the class cache and the background compiler thread. The thread is shut
/// down and joined when the last native class cache is dropped.
struct Inner {
    cache: Arc<Cache>,
    compiler_tx: Option<std::sync::mpsc::Sender<CompilerInput>>,
    compiler_thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Close the channel to force the compiler thread out of its loop.
        drop(self.compiler_tx.take());
        // Now wait for the compiler thread to end.
        if let Some(thread) = self.compiler_thread.take() {
            let _ = thread.join();
        }
    }
}

impl NativeClassCache {
    pub fn spawn(cache_size: NonZeroUsize, optimization_level: u8) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();

        let cache = Arc::new(Mutex::new(SizedCache::with_size(cache_size.get())));

        let compiler_handle = util::task::spawn_std({
            let cache = Arc::clone(&cache);
            move |cancellation_token| {
                compiler_thread(cache, rx, cancellation_token, optimization_level.into())
            }
        });

        NativeClassCache {
            inner: Arc::new(Inner {
                cache,
                compiler_tx: Some(tx),
                compiler_thread: Some(compiler_handle),
            }),
        }
    }

    pub fn get(
        &self,
        class_hash: ClassHash,
        sierra_version: SierraVersion,
        class_definition: SerializedOpaqueClassDefinition,
        casm_definition: SerializedCasmDefinition,
    ) -> Option<NativeCompiledClassV1> {
        let mut locked = self.inner.cache.lock().unwrap();

        match locked.cache_get(&class_hash) {
            Some(CacheItem::CompiledClass(cached_class)) => {
                tracing::trace!(%class_hash, "Native class cache hit");
                metrics::counter!("native_class_cache_hit_total").increment(1);
                Some(cached_class.clone())
            }
            Some(CacheItem::CompilationPending) => {
                tracing::trace!(%class_hash, "Native class cache miss (pending)");
                metrics::counter!("native_class_cache_miss_compilation_pending_total").increment(1);
                None
            }
            None => {
                tracing::trace!(%class_hash, "Native class cache miss (compiling)");
                metrics::counter!("native_class_cache_miss_total").increment(1);
                locked.cache_set(class_hash, CacheItem::CompilationPending);
                let _ = self
                    .inner
                    .compiler_tx
                    .as_ref()
                    .expect("compiler channel is open")
                    .send(CompilerInput {
                        class_hash,
                        sierra_version,
                        class_definition,
                        casm_definition,
                    });
                metrics::gauge!(NATIVE_CLASS_COMPILATION_QUEUED_TOTAL_METRIC_NAME).increment(1.0);
                None
            }
        }
    }
}

const NATIVE_CLASS_COMPILATION_QUEUED_TOTAL_METRIC_NAME: &str =
    "native_class_compilation_queued_total";

fn compiler_thread(
    cache: Arc<Cache>,
    rx: std::sync::mpsc::Receiver<CompilerInput>,
    cancellation_token: CancellationToken,
    optimization_level: cairo_native::OptLevel,
) {
    loop {
        if cancellation_token.is_cancelled() {
            return;
        }

        let Ok(input) = rx.recv() else {
            return;
        };

        let class_hash = input.class_hash;

        let _span =
            tracing::span!(tracing::Level::DEBUG, "native_class_compiler", %class_hash).entered();

        tracing::debug!("Compiling native class");
        let started_at = std::time::Instant::now();
        match sierra_class_as_native(input, optimization_level) {
            Ok(compiled_class) => {
                let elapsed = started_at.elapsed();
                tracing::debug!(?elapsed, "Compilation finished");
                metrics::histogram!("native_class_compilation_duration_seconds",)
                    .record(elapsed.as_secs_f64());
                metrics::counter!("native_class_compiled_total").increment(1);
                cache
                    .lock()
                    .unwrap()
                    .cache_set(class_hash, CacheItem::CompiledClass(compiled_class));
            }
            Err(error) => {
                tracing::error!(elapsed=?started_at.elapsed(), %error, "Error compiling native class");
                metrics::counter!("native_class_compilation_errors_total").increment(1);
            }
        }

        metrics::gauge!(NATIVE_CLASS_COMPILATION_QUEUED_TOTAL_METRIC_NAME).decrement(1.0);
    }
}

fn sierra_class_as_native(
    input: CompilerInput,
    optimization_level: cairo_native::OptLevel,
) -> Result<NativeCompiledClassV1, StateError> {
    let mut sierra_definition: serde_json::Value =
        serde_json::from_slice(input.class_definition.as_slice())
            .map_err(|e| StateError::ProgramError(ProgramError::Parse(e)))?;
    let sierra_abi_str = sierra_definition
        .get("abi")
        .ok_or_else(|| StateError::StateReadError("Sierra ABI is missing".to_owned()))?
        .as_str()
        .ok_or_else(|| StateError::StateReadError("Sierra ABI is not a string".to_owned()))?;
    sierra_definition["abi"] = serde_json::from_str(sierra_abi_str)
        .map_err(|e| StateError::ProgramError(ProgramError::Parse(e)))?;

    let sierra_class: cairo_lang_starknet_classes::contract_class::ContractClass =
        serde_json::from_value(sierra_definition)
            .map_err(|e| StateError::ProgramError(ProgramError::Parse(e)))?;

    let sierra_program = sierra_class.extract_sierra_program(false).map_err(|e| {
        StateError::StateReadError(format!(
            "Error parsing Sierra
                program: {e}"
        ))
    })?;

    let version_id = cairo_lang_starknet_classes::compiler_version::VersionId {
        major: input.sierra_version.major as usize,
        minor: input.sierra_version.minor as usize,
        patch: input.sierra_version.patch as usize,
    };

    let contract_executor = std::panic::catch_unwind(|| {
        let mut stats = cairo_native::statistics::Statistics::default();
        let executor = AotContractExecutor::new(
            &sierra_program.program,
            &sierra_class.entry_points_by_type,
            version_id,
            optimization_level,
            // `stats` - Passing a [cairo_native::statistics::Statistics] object enables collecting
            // compilation statistics.
            Some(&mut stats),
        );
        update_compiler_metrics(&stats);
        executor
    })
    .map_err(|e| StateError::StateReadError(format!("Error compiling native class: {e:?}")))?
    .map_err(|e| StateError::StateReadError(format!("Error compiling native class: {e}")))?;

    let casm_definition =
        std::str::from_utf8(input.casm_definition.as_slice()).map_err(|error| {
            StateError::StateReadError(format!("Class definition is not valid UTF-8: {error}"))
        })?;

    let casm_class = blockifier::execution::contract_class::CompiledClassV1::try_from_json_string(
        casm_definition,
        input.sierra_version,
    )
    .map_err(StateError::ProgramError)?;

    let native_class = NativeCompiledClassV1::new(contract_executor, casm_class);

    Ok(native_class)
}

fn update_compiler_metrics(stats: &cairo_native::statistics::Statistics) {
    if let Some(sierra_to_mlir_time_ms) = stats.compilation_sierra_to_mlir_time_ms {
        metrics::histogram!("native_class_compilation_sierra_to_mlir_duration_seconds")
            .record(sierra_to_mlir_time_ms as f64 / 1000.0)
    }
    if let Some(mlir_passes_time_ms) = stats.compilation_mlir_passes_time_ms {
        metrics::histogram!("native_class_compilation_mlir_passes_duration_seconds")
            .record(mlir_passes_time_ms as f64 / 1000.0)
    }
    if let Some(mlir_to_llvm_time_ms) = stats.compilation_mlir_to_llvm_time_ms {
        metrics::histogram!("native_class_compilation_mlir_to_llvm_duration_seconds")
            .record(mlir_to_llvm_time_ms as f64 / 1000.0)
    }
    if let Some(llvm_passes_time_ms) = stats.compilation_llvm_passes_time_ms {
        metrics::histogram!("native_class_compilation_llvm_passes_duration_seconds")
            .record(llvm_passes_time_ms as f64 / 1000.0)
    }
    if let Some(llvm_to_object_time_ms) = stats.compilation_llvm_to_object_time_ms {
        metrics::histogram!("native_class_compilation_llvm_to_object_duration_seconds")
            .record(llvm_to_object_time_ms as f64 / 1000.0)
    }
    if let Some(linking_time_ms) = stats.compilation_linking_time_ms {
        metrics::histogram!("native_class_compilation_linking_duration_seconds")
            .record(linking_time_ms as f64 / 1000.0)
    }
    if let Some(object_size_bytes) = stats.object_size_bytes {
        metrics::histogram!("native_class_compilation_object_size_bytes")
            .record(object_size_bytes as f64);
    }
}

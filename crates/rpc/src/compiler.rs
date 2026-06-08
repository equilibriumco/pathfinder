use std::num::NonZeroUsize;
use std::sync::Arc;

use pathfinder_common::class_definition::{SerializedCasmDefinition, SerializedSierraDefinition};

/// Sierra to CASM compiler wrapper for the RPC layer.
#[derive(Clone)]
pub struct PathfinderCompiler {
    concurrency_limit: Arc<util::sync::Semaphore>,
    resource_limits: pathfinder_compiler::ResourceLimits,
    blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
}

impl PathfinderCompiler {
    /// Creates a new Sierra to CASM compiler.
    pub fn new(
        concurrency_limit: NonZeroUsize,
        resource_limits: pathfinder_compiler::ResourceLimits,
        blockifier_libfuncs: pathfinder_compiler::BlockifierLibfuncs,
    ) -> Self {
        Self {
            concurrency_limit: Arc::new(util::sync::Semaphore::new(concurrency_limit.get())),
            resource_limits,
            blockifier_libfuncs,
        }
    }

    /// Compiles a Sierra definition to CASM.
    ///
    /// This is a blocking function. When used inside an async runtime, it
    /// should be called on a blocking thread.
    pub fn compile_sierra_to_casm(
        &self,
        sierra_definition: &SerializedSierraDefinition,
    ) -> anyhow::Result<SerializedCasmDefinition> {
        let _permit = self.concurrency_limit.acquire();
        pathfinder_compiler::compile_sierra_to_casm(
            sierra_definition,
            self.resource_limits,
            self.blockifier_libfuncs,
        )
    }
}
